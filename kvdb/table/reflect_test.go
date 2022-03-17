package table

//go:generate go run github.com/golang/mock/mockgen -package=table -destination=mock_test.go github.com/Fantom-foundation/lachesis-base/kvdb DBProducer,DropableStore
