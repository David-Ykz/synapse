package broker

type Cache struct {
	stateMap map[string]interface{}
}

func NewCache() *Cache {
	return &Cache{
		stateMap: make(map[string]interface{}),
	}
}

func (c *Cache) Add(requestId string, state interface{}) {
	c.stateMap[requestId] = state
}

func (c *Cache) Get(requestId string) interface{} {
	return c.stateMap[requestId]
}

func (c *Cache) Contains(requestId string) bool {
	_, exists := c.stateMap[requestId]
	return exists
}

func (c *Cache) Delete(requestId string) {
	delete(c.stateMap, requestId)
}
