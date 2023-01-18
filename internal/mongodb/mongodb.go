package mongodb

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"sync"
	"time"
)

var amount int
var poolCount int
var countInWorker int
var usersIdContainer Container
var articlesIdContainer Container
var commandCounter = 0
var loremText = "Lorem Ipsum - это текст-\"рыба\", часто используемый в печати и вэб-дизайне. Lorem Ipsum является стандартной \"рыбой\" для текстов на латинице с начала XVI века. В то время некий безымянный печатник создал большую коллекцию размеров и форм шрифтов, используя Lorem Ipsum для распечатки образцов. Lorem Ipsum не только успешно пережил без заметных изменений пять веков, но и перешагнул в электронный дизайн. Его популяризации в новое время послужили публикация листов Letraset с образцами Lorem Ipsum в 60-х годах и, в более недавнее время, программы электронной вёрстки типа Aldus PageMaker, в шаблонах которых используется Lorem Ipsum."

var wg sync.WaitGroup

func StartTest(amountRows, poolCountSize, passTestCount int) {
	amount = amountRows
	poolCount = poolCountSize

	countInWorker = int(amount / poolCount)

	client, ctx, cancel, err := connect("mongodb://root:root@0.0.0.0:27017/?authSource=admin")
	if err != nil {
		panic(err)
	}

	defer closeDb(client, ctx, cancel)
	defer resetDB(client, ctx)

	usersIdContainer = *NewContainer()
	articlesIdContainer = *NewContainer()

	// Ping mongoDB with Ping method
	err = ping(client, ctx)
	if err != nil {
		panic(err)
	}

	// add users
	if passTestCount < 1 {
		insertUsers(client, ctx)
	}
	// add articles
	if passTestCount < 2 {
		insertArticles(client, ctx)
	}
	// add comments
	if passTestCount < 3 {
		insertComments(client, ctx)
	}

	// select users
	if passTestCount < 4 {
		selectFromIdUsers(client, ctx)
	}

	// select with joins
	//selectWithJoins(db)

	// select with filter
	//selectWithFilters(db)

	// select with joins and filters
	//selectWithJoinsAndFilters(db)

	// add nullable column
	//addNullableColumn(db)

	// add column with default value
	//addNullableWithDefault(db)

	// drop column test
	//dropColumn(db)

	// multiline insert
	//multilineInsertArticles(db)

	// bulk insert
	//bulkCopy(db)
}

func closeDb(client *mongo.Client, ctx context.Context,
	cancel context.CancelFunc) {

	// CancelFunc to cancel to context
	defer cancel()

	// client provides a method to closeDb
	// a mongoDB connection.
	defer func() {

		// client.Disconnect method also has deadline.
		// returns error if any,
		if err := client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()
}

func resetDB(client *mongo.Client, ctx context.Context) {
	err := client.Database("test").Drop(ctx)
	if err != nil {
		panic(err)
	}
}

func connect(uri string) (*mongo.Client, context.Context,
	context.CancelFunc, error) {

	// ctx will be used to set deadline for process, here
	// deadline will of 30 seconds.
	ctx, cancel := context.WithTimeout(context.Background(),
		30*time.Second)

	// mongo.Connect return mongo.Client method
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	return client, ctx, cancel, err
}

func ping(client *mongo.Client, ctx context.Context) error {

	// mongo.Client has Ping to ping mongoDB, deadline of
	// the Ping method will be determined by cxt
	// Ping method return error if any occurred, then
	// the error can be handled.
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return err
	}
	fmt.Println("connected successfully")
	return nil
}

func insertUsers(client *mongo.Client, ctx context.Context) {
	start := time.Now()
	log.Print("========== INSERT ============")
	log.Printf("Insert %d users in progress...", amount)
	log.Printf("Use connection pool size = %d", poolCount)

	collection := client.Database("test").Collection("users")

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(collection *mongo.Collection, countInWorker, i int) {
			defer wg.Done()
			maxDiapason := (i + 1) * countInWorker
			for currentPosition := i * countInWorker; currentPosition < maxDiapason; currentPosition++ {
				name := fmt.Sprint("name_", currentPosition)
				descr := fmt.Sprint("descr_", currentPosition)

				result, err := collection.InsertOne(ctx, bson.D{
					{"name", name},
					{"description", descr},
				})
				if err != nil {
					panic(err)
				}

				usersIdContainer.Add(currentPosition, result.InsertedID.(primitive.ObjectID).Hex())
			}
		}(collection, countInWorker, i)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func insertArticles(client *mongo.Client, ctx context.Context) {
	start := time.Now()
	log.Print("========== INSERT ARTICLES ============")
	log.Printf("Insert %d articles in progress...", amount)
	log.Printf("Use connection pool size = %d", poolCount)

	collection := client.Database("test").Collection("articles")

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(collection *mongo.Collection, countInWorker, i int) {
			var objectID primitive.ObjectID
			var err error
			defer wg.Done()
			maxDiapason := (i + 1) * countInWorker
			for currentPosition := i * countInWorker; currentPosition < maxDiapason; currentPosition++ {
				title := fmt.Sprint("title_", currentPosition)

				objectID, err = primitive.ObjectIDFromHex(usersIdContainer.GetByKey(currentPosition))
				if err != nil {
					panic(err)
				}

				result, err := collection.InsertOne(ctx, &Article{
					ID:          primitive.NewObjectID(),
					AuthorId:    objectID,
					Title:       title,
					Description: loremText,
				})
				if err != nil {
					panic(err)
				}

				articlesIdContainer.Add(currentPosition, result.InsertedID.(primitive.ObjectID).Hex())
			}
		}(collection, countInWorker, i)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func insertComments(client *mongo.Client, ctx context.Context) {
	start := time.Now()
	log.Print("========== INSERT COMMENTS ============")
	log.Printf("Insert %d users in progress...", amount)
	log.Printf("Use connection pool size = %d", poolCount)

	collection := client.Database("test").Collection("comments")

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(collection *mongo.Collection, countInWorker, i int) {
			var objectIDUser primitive.ObjectID
			var objectIDComment primitive.ObjectID
			var err error

			defer wg.Done()
			maxDiapason := (i + 1) * countInWorker

			for currentPosition := i * countInWorker; currentPosition < maxDiapason; currentPosition++ {
				title := fmt.Sprint("title_", currentPosition)

				objectIDUser, err = primitive.ObjectIDFromHex(usersIdContainer.GetByKey(currentPosition))
				if err != nil {
					panic(err)
				}

				objectIDComment, err = primitive.ObjectIDFromHex(articlesIdContainer.GetByKey(currentPosition))
				if err != nil {
					panic(err)
				}

				result, err := collection.InsertOne(ctx, &Comment{
					ID:        primitive.NewObjectID(),
					ArticleId: objectIDComment,
					AuthorId:  objectIDUser,
					Title:     title,
					Text:      loremText,
				})
				if err != nil {
					panic(err)
				}

				articlesIdContainer.Add(currentPosition, result.InsertedID.(primitive.ObjectID).Hex())
			}
		}(collection, countInWorker, i)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func selectFromIdUsers(client *mongo.Client, ctx context.Context) {
	start := time.Now()
	log.Print("======= SELECT FROM ID =======")
	log.Printf("Select %d users in progress...", amount)

	collection := client.Database("test").Collection("users")

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(collection *mongo.Collection, countInWorker, i int) {
			var _ error

			defer wg.Done()
			maxDiapason := (i + 1) * countInWorker

			for currentPosition := i * countInWorker; currentPosition < maxDiapason; currentPosition++ {
				oid, err := primitive.ObjectIDFromHex(usersIdContainer.GetByKey(currentPosition))
				if err != nil {
					panic(err)
				}

				filter := bson.M{"_id": oid}

				result := collection.FindOne(ctx, filter)
				if result.Err() != nil {
					if errors.Is(result.Err(), mongo.ErrNoDocuments) {
						panic("document not found")
					}
					panic("failed to find one user by id: %s due to error: %v")
				}
			}
		}(collection, countInWorker, i)
	}

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}
