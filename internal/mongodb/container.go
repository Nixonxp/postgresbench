package mongodb

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"sync"
)

type Proximity map[int]string

type Container struct {
	mx sync.RWMutex
	m  map[int]string
}

type Article struct {
	ID          primitive.ObjectID `bson:"_id"`
	AuthorId    primitive.ObjectID `bson:"author_id"`
	Title       string             `bson:"title"`
	Description string             `bson:"description"`
}

type Comment struct {
	ID        primitive.ObjectID `bson:"_id"`
	ArticleId primitive.ObjectID `bson:"article_id"`
	AuthorId  primitive.ObjectID `bson:"author_id"`
	Title     string             `bson:"title"`
	Text      string             `bson:"text"`
}

func (c *Container) Add(key int, objectId string) {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.m[key] = objectId
}

func (c *Container) GetByKey(key int) string {
	c.mx.RLock()
	defer c.mx.RUnlock()

	return c.m[key]
}

func NewContainer() *Container {
	return &Container{
		m: make(map[int]string),
	}
}
