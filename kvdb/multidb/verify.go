package multidb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/log"
)

func (p *Producer) verifyRecords(oldDBRecords map[DBLocator][]TableRecord) error {
	for oldLoc, records := range oldDBRecords {
		for _, old := range records {
			newRoute := p.RouteOf(old.Req)
			if oldLoc.Type != newRoute.Type {
				return fmt.Errorf("DB type for '%s' doesn't match to previous instance: '%s' != '%s'", old.Req, newRoute.Type, oldLoc.Type)
			}
			if oldLoc.Name != newRoute.Name {
				return fmt.Errorf("DB name for '%s' doesn't match to previous instance: '%s' != '%s'", old.Req, newRoute.Name, oldLoc.Name)
			}
			if old.Table != newRoute.Table {
				return fmt.Errorf("table for '%s' doesn't match to previous instance: '%s' != '%s'", old.Req, newRoute.Table, old.Table)
			}
		}
	}
	return nil
}

func (p *Producer) getRecords() (map[DBLocator][]TableRecord, error) {
	dbRecords := make(map[DBLocator][]TableRecord)
	for typ, producer := range p.allProducers {
		for _, name := range producer.Names() {
			db, err := producer.OpenDB(name)
			if err != nil {
				return nil, fmt.Errorf("failed to open DB %s: %w", name, err)
			}

			err = func() error {
				defer func() {
					innerErr := db.Close()
					log.Error(fmt.Sprintf("can't close the DB: producerType %s, name %s, error %v", name, typ, innerErr))
				}()

				records, err := ReadTablesList(db, p.tableRecordsKey)
				if err != nil {
					return fmt.Errorf("failed to read tables for %s: %w", name, err)
				}

				locator := DBLocator{
					Type: typ,
					Name: name,
				}
				dbRecords[locator] = records

				return nil
			}()

			if err != nil {
				return nil, err
			}
		}
	}
	return dbRecords, nil
}

func (p *Producer) Verify() error {
	dbRecords, err := p.getRecords()
	if err != nil {
		return err
	}
	return p.verifyRecords(dbRecords)
}
