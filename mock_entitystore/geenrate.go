package mock_entitystore

//go:generate mockgen -source=../aggregation.go -destination=mock_aggregation.go
//go:generate mockgen -source=../datastore_client.go -destination=mock_datastore_client.go
//go:generate mockgen -source=../entity_lister.go -destination=mock_entity_lister.go
//go:generate mockgen -source=../query.go -destination=mock_query.go
//go:generate mockgen -source=../mutation.go -destination=mock_mutation.go
