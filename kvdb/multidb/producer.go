package multidb

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/table"
)

type Producer struct {
	routingTable    map[string]Route
	routingRegexps  []regexpRoute
	producers       map[TypeName]kvdb.FullDBProducer
	allProducers    map[TypeName]kvdb.FullDBProducer
	tableRecordsKey []byte
}

// NewProducer of a combined producer for multiple types of DBs.
func NewProducer(producers map[TypeName]kvdb.FullDBProducer, routingTable map[string]Route, routingRegexpsMap map[string]Route, tableRecordsKey []byte) *Producer {
	if _, ok := routingTable[""]; !ok {
		panic("default route must always be defined")
	}
	// compile regular expressions
	routingRegexps := make([]regexpRoute, 0, len(routingRegexpsMap))
	used := make(map[TypeName]kvdb.FullDBProducer)
	for regexpRule, route := range routingRegexpsMap {
		rx, err := regexp.Compile(regexpRule)
		if err != nil {
			panic(err)
		}
		routingRegexps = append(routingRegexps, regexpRoute{
			regexp: rx,
			route:  route,
		})
		used[route.Type] = producers[route.Type]
	}
	for _, route := range routingTable {
		used[route.Type] = producers[route.Type]
	}
	return &Producer{
		producers:       used,
		allProducers:    producers,
		routingTable:    routingTable,
		routingRegexps:  routingRegexps,
		tableRecordsKey: tableRecordsKey,
	}
}

func (p *Producer) RouteOf(req string) Route {
	rightPartTable := ""
	rightPartName := ""
	for {
		dest, ok := p.routingTable[req]
		for i := 0; !ok && i < len(p.routingRegexps); i++ {
			// try regexp
			if p.routingRegexps[i].regexp.MatchString(req) {
				dest = p.routingRegexps[i].route
				ok = true
			}
		}
		if ok {
			return Route{
				Type:   dest.Type,
				Name:   dest.Name + rightPartName,
				Table:  dest.Table + rightPartTable,
				NoDrop: dest.NoDrop,
			}
		}

		slashPos := strings.LastIndexByte(req, '/')
		if slashPos < 0 {
			// if root, then it refers to DB name, not table
			rightPartName = req
			req = ""
		} else {
			rightPartTable += req[slashPos+1:]
			req = req[:slashPos]
		}
	}
}

func tablesConflicting(a, b string) bool {
	return strings.HasPrefix(a, b) || strings.HasPrefix(b, a)
}

func (p *Producer) handleRoute(db kvdb.Store, req string, route Route) error {
	records, err := ReadTablesList(db, p.tableRecordsKey)
	if err != nil {
		return err
	}
	for _, old := range records {
		if old.Req == req && old.Table == route.Table {
			return nil
		}
		if old.Req == req && old.Table != route.Table {
			return fmt.Errorf("DB %s/%s, re-assigning table for req %s: new='%s' != old='%s'", route.Type, route.Name, req, route.Table, old.Table)
		}
		if tablesConflicting(old.Table, route.Table) {
			return fmt.Errorf("DB %s/%s, conflicting tables for reqs: new=%s:'%s'~old=%s:'%s'", route.Type, route.Name, req, route.Table, old.Req, old.Table)
		}
	}
	// not found
	records = append(records, TableRecord{
		Req:   req,
		Table: route.Table,
	})
	return WriteTablesList(db, p.tableRecordsKey, records)
}

// OpenDB or create db with name.
func (p *Producer) OpenDB(req string) (kvdb.Store, error) {
	route := p.RouteOf(req)

	producer := p.producers[route.Type]
	if producer == nil {
		return nil, fmt.Errorf("missing producer '%s'", route.Type)
	}
	db, err := producer.OpenDB(route.Name)
	if err != nil {
		return nil, err
	}
	err = p.handleRoute(db, req, route)
	if err != nil {
		return nil, err
	}
	cdb := &closableTable{
		Store:      db,
		underlying: db,
		noDrop:     route.NoDrop,
	}
	if len(route.Table) != 0 {
		cdb.Store = table.New(db, []byte(route.Table))
	}
	return cdb, nil
}

// Names of existing databases.
func (p *Producer) Names() []string {
	// TODO return list of tables
	res := make([]string, 0, 20)
	for _, producer := range p.producers {
		res = append(res, producer.Names()...)
	}
	return res
}

func (p *Producer) NotFlushedSizeEst() int {
	res := 0
	for _, producer := range p.producers {
		res += producer.NotFlushedSizeEst()
	}
	return res
}

func (p *Producer) Flush(id []byte) error {
	for _, producer := range p.producers {
		err := producer.Flush(id)
		if err != nil {
			return err
		}
	}
	return nil
}
