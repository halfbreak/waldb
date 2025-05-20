package main

func main() {
	db, err := NewKVS()
	defer db.Close()
	if err != nil {
		panic(err)
	}
	db.Add("123", []byte("{ 'id': 123, 'name': 'Entity 1'}"))
	db.Add("123", []byte("{ 'id': 123, 'name': 'New Entity 1'}"))
	db.Add("124", []byte("{ 'id': 124, 'name': 'Entity 2'}"))

	entity1, err := db.Read("123")
	if err != nil {
		panic(err)
	}
	entity2, err := db.Read("124")
	if err != nil {
		panic(err)
	}
	println(string(entity1))
	println(string(entity2))
}
