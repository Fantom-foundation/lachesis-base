package multidb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/table"
	"github.com/Fantom-foundation/lachesis-base/utils/fmtfilter"
)

type Producer struct {
	routingTable    map[string]Route
	routingFmt      []scanfRoute
	usedProducers   map[TypeName]kvdb.FullDBProducer
	allProducers    map[TypeName]kvdb.FullDBProducer
	tableRecordsKey []byte
}

// NewProducer of a combined producer for multiple types of DBs.
func NewProducer(producers map[TypeName]kvdb.FullDBProducer, routingTable map[string]Route, tableRecordsKey []byte) (*Producer, error) {
	if _, ok := routingTable[""]; !ok {
		return nil, errors.New("default route must always be defined")
	}
	// compile regular expressions
	routingFmt := make([]scanfRoute, 0, len(routingTable))
	exactRoutingTable := make(map[string]Route, len(routingTable))
	used := make(map[TypeName]kvdb.FullDBProducer)
	for req, route := range routingTable {
		used[route.Type] = producers[route.Type]
		if !strings.ContainsRune(req, '%') && !strings.ContainsRune(route.Name, '%') {
			exactRoutingTable[req] = route
			continue
		}
		fn, err := fmtfilter.CompileFilter(req, route.Name)
		if err != nil {
			return nil, err
		}

		routingFmt = append(routingFmt, scanfRoute{
			Name:   fn,
			Type:   route.Type,
			Table:  route.Table,
			NoDrop: route.NoDrop,
		})
	}
	return &Producer{
		usedProducers:   used,
		allProducers:    producers,
		routingTable:    exactRoutingTable,
		routingFmt:      routingFmt,
		tableRecordsKey: tableRecordsKey,
	}, nil
}

func (p *Producer) RouteOf(req string) Route {
	rightPartTable := ""
	rightPartName := ""
	for {
		dest, ok := p.routingTable[req]
		for i := 0; !ok && i < len(p.routingFmt); i++ {
			// try scanf
			if name, err := p.routingFmt[i].Name(req); err == nil {
				dest = Route{
					Type:   p.routingFmt[i].Type,
					Name:   name,
					Table:  p.routingFmt[i].Table,
					NoDrop: p.routingFmt[i].NoDrop,
				}
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

	producer := p.usedProducers[route.Type]
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
	for _, producer := range p.usedProducers {
		res = append(res, producer.Names()...)
	}
	return res
}

func (p *Producer) NotFlushedSizeEst() int {
	res := 0
	for _, producer := range p.usedProducers {
		res += producer.NotFlushedSizeEst()
	}
	return res
}

func (p *Producer) Flush(id []byte) error {
	for _, producer := range p.usedProducers {
		err := producer.Flush(id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) Initialize(dbNames []string, flushID []byte) ([]byte, error) {
	for _, producer := range p.allProducers {
		var err error
		flushID, err = producer.Initialize(dbNames, flushID)
		if err != nil {
			return flushID, err
		}
	}
	return flushID, nil
}

func (p *Producer) Close() error {
	for _, producer := range p.allProducers {
		err := producer.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
