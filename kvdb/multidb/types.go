package multidb

type TypeName string

type Route struct {
	Type   TypeName
	Name   string
	Table  string
	NoDrop bool
}

type scanfRoute struct {
	Name   func(req string) (string, error)
	Type   TypeName
	Table  string
	NoDrop bool
}

type DBLocator struct {
	Type TypeName
	Name string
}

func DBLocatorOf(r Route) DBLocator {
	return DBLocator{
		Type: r.Type,
		Name: r.Name,
	}
}

type TableLocator struct {
	Type  TypeName
	Name  string
	Table string
}

func TableLocatorOf(r Route) TableLocator {
	return TableLocator{
		Type:  r.Type,
		Name:  r.Name,
		Table: r.Table,
	}
}
