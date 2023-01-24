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
	"math/rand"
	"sync"
	"time"
)

import mog "go.mongodb.org/mongo-driver/mongo"

var amount int
var poolCount int
var countInWorker int
var usersIdContainer Container
var articlesIdContainer Container
var isUseTestSchema = false
var loremText = "Lorem Ipsum - это текст-\"рыба\", часто используемый в печати и вэб-дизайне. Lorem Ipsum является стандартной \"рыбой\" для текстов на латинице с начала XVI века. В то время некий безымянный печатник создал большую коллекцию размеров и форм шрифтов, используя Lorem Ipsum для распечатки образцов. Lorem Ipsum не только успешно пережил без заметных изменений пять веков, но и перешагнул в электронный дизайн. Его популяризации в новое время послужили публикация листов Letraset с образцами Lorem Ipsum в 60-х годах и, в более недавнее время, программы электронной вёрстки типа Aldus PageMaker, в шаблонах которых используется Lorem Ipsum."

var wg sync.WaitGroup

func StartTest(amountRows, poolCountSize, passTestCount, useTestSchema int) {
	amount = amountRows
	poolCount = poolCountSize

	if useTestSchema != 0 {
		isUseTestSchema = true
	}

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
	if passTestCount < 5 {
		selectWithJoins(client, ctx)
	}

	// select with filter
	if passTestCount < 6 {
		selectWithFilters(client, ctx)
	}

	// select with joins and filters
	if passTestCount < 7 {
		selectWithJoinsAndFilters(client, ctx)
	}

	// add nullable column
	if passTestCount < 8 {
		addNullableColumn(client, ctx)
	}

	// add column with default value
	if passTestCount < 9 {
		addNullableWithDefault(client, ctx)
	}

	// drop column test
	if passTestCount < 10 {
		dropColumn(client, ctx)
	}

	// bulk insert
	if passTestCount < 11 {
		bulkCopy(client, ctx)
	}
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
		30*time.Hour)

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
	if isUseTestSchema == true {
		amount = 100000
	}
	countInWorker = int(amount / poolCount)
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
				name := fmt.Sprint("user_", currentPosition)
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

	err := AddIndex(collection, ctx, "_id")
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func AddIndex(collection *mongo.Collection, ctx context.Context, indexKey string) error {
	indexName, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.M{
			indexKey: 1, // index in ascending order
		},
	})
	if err != nil {
		return err
	}
	fmt.Println(indexName)
	return nil
}

func insertArticles(client *mongo.Client, ctx context.Context) {
	if isUseTestSchema == true {
		amount = 1000000
	}
	countInWorker = int(amount / poolCount)
	var authorId int

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
				title := fmt.Sprint("article_", currentPosition)

				authorId = int(currentPosition / 100)
				objectID, err = primitive.ObjectIDFromHex(usersIdContainer.GetByKey(authorId))
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

	err := AddIndex(collection, ctx, "_id")
	if err != nil {
		panic(err)
	}

	err = AddIndex(collection, ctx, "author_id")
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func insertComments(client *mongo.Client, ctx context.Context) {
	if isUseTestSchema == true {
		amount = 10000000
	}
	countInWorker = int(amount / poolCount)
	var authorId int
	var articleId int

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
				title := fmt.Sprint("comment_", currentPosition)

				authorId = int(currentPosition / 1000)
				articleId = int(currentPosition / 1000)

				objectIDUser, err = primitive.ObjectIDFromHex(usersIdContainer.GetByKey(authorId))
				if err != nil {
					panic(err)
				}

				objectIDComment, err = primitive.ObjectIDFromHex(articlesIdContainer.GetByKey(articleId))
				if err != nil {
					panic(err)
				}

				_, err := collection.InsertOne(ctx, &Comment{
					ID:        primitive.NewObjectID(),
					ArticleId: objectIDComment,
					AuthorId:  objectIDUser,
					Title:     title,
					Text:      loremText,
				})
				if err != nil {
					panic(err)
				}

			}
		}(collection, countInWorker, i)
	}

	wg.Wait()

	articlesIdContainer = *NewContainer()

	err := AddIndex(collection, ctx, "_id")
	if err != nil {
		panic(err)
	}

	err = AddIndex(collection, ctx, "author_id")
	if err != nil {
		panic(err)
	}

	err = AddIndex(collection, ctx, "article_id")
	if err != nil {
		panic(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted %d rows in %s", amount, elapsed)
	log.Print("==============================")
}

func selectFromIdUsers(client *mongo.Client, ctx context.Context) {
	if isUseTestSchema == true {
		amount = 100000
	}
	countInWorker = int(amount / poolCount)

	log.Print("======= SELECT FROM ID =======")
	log.Printf("Select %d users in progress...", amount)

	collection := client.Database("test").Collection("users")

	var results []int
	var result float64

	for i := 0; i < poolCount; i++ {
		wg.Add(1)
		go func(collection *mongo.Collection, countInWorker, i int) {
			var _ error

			defer wg.Done()
			start := time.Now()

			rand.Seed(time.Now().UnixNano())
			id := rand.Intn(amount-0+1) + 0

			oid, err := primitive.ObjectIDFromHex(usersIdContainer.GetByKey(id))
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
			t := time.Now()
			elapsed := t.Sub(start).Milliseconds()

			results = append(results, int(elapsed))
		}(collection, countInWorker, i)
	}

	wg.Wait()

	result = float64(sum(results)) / float64(poolCount)

	log.Printf("Average time for 1 row from id in %v ms", result)
	log.Print("==============================")
}

func sum(arr []int) int {
	sum := 0
	for _, valueInt := range arr {
		sum += valueInt
	}
	return sum
}

func selectWithJoins(client *mongo.Client, ctx context.Context) {
	start := time.Now()
	log.Print("======= SELECT ALL WITH JOIN =======")
	log.Printf("Select rows with join ($lookup) in progress...")

	collection := client.Database("test").Collection("users")

	lookupStageArticle := bson.D{
		{"$lookup", bson.D{{"from", "articles"}, {"localField", "_id"}, {"foreignField", "author_id"}, {"as", "author"}}}}

	lookupStageComments := bson.D{
		{"$lookup", bson.D{{"from", "comments"}, {"localField", "_id"}, {"foreignField", "author_id"}, {"as", "comments"}}}}

	limitStage := bson.D{{"$limit", 10}}

	showLoadedStructCursor, err := collection.Aggregate(ctx, mongo.Pipeline{lookupStageArticle, lookupStageComments, limitStage})
	if err != nil {
		panic(err)
	}

	countRows := 0
	for showLoadedStructCursor.Next(ctx) {
		countRows++
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Selected all with join %d rows in %s", countRows, elapsed)
	log.Print("==============================")
}

func selectWithFilters(client *mongo.Client, ctx context.Context) {
	start := time.Now()
	log.Print("======= SELECT WITH FILTER =======")
	log.Printf("Select users collection rows with filter in progress...")

	collection := client.Database("test").Collection("users")

	filter := bson.D{
		{"name", primitive.Regex{Pattern: "er_1", Options: ""}},
		{"description", primitive.Regex{Pattern: "scr_1", Options: ""}},
	}

	optionsFind := options.Find()
	optionsFind.SetSort(bson.M{"name": 1})
	optionsFind.SetSkip(0)
	optionsFind.SetLimit(10)

	showLoadedStructCursor, err := collection.Find(ctx, filter, optionsFind)
	if err != nil {
		panic(err)
	}

	countRows := 0
	for showLoadedStructCursor.Next(ctx) {
		countRows++
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Selected with filter %d rows in %s", countRows, elapsed)
	log.Print("==============================")
}

func selectWithJoinsAndFilters(client *mongo.Client, ctx context.Context) {
	start := time.Now()
	log.Print("======= SELECT ALL WITH JOIN AND FILTERS =======")
	log.Printf("Select rows with join (lookup) and filters (pipelines) in progress...")

	collection := client.Database("test").Collection("users")

	lookupStageArticle := bson.D{
		{"$lookup", bson.D{{"from", "articles"}, {"localField", "_id"}, {"foreignField", "author_id"}, {"as", "author"}}}}

	lookupStageComments := bson.D{
		{"$lookup", bson.D{{"from", "comments"}, {"localField", "_id"}, {"foreignField", "author_id"}, {"as", "comments"}}}}

	filterUsers := bson.D{{"$match", bson.D{{"name", bson.D{{"$regex", "er_1"}}}}}}

	limitStage := bson.D{{"$limit", 10}}

	showLoadedStructCursor, err := collection.Aggregate(ctx, mongo.Pipeline{lookupStageArticle, lookupStageComments, filterUsers, limitStage})
	if err != nil {
		panic(err)
	}

	countRows := 0
	for showLoadedStructCursor.Next(ctx) {
		countRows++
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Selected with filter %d rows in %s", countRows, elapsed)
	log.Print("==============================")
}

func addNullableColumn(client *mongo.Client, ctx context.Context) {
	start := time.Now()
	log.Print("======= ADD NULLABLE COLUMN =======")
	log.Printf("Insert nullable column in progress...")

	collection := client.Database("test").Collection("users")

	filter := bson.D{{}}
	pipe := bson.D{{"$set", bson.M{"nullable": nil}}}
	res, err := collection.UpdateMany(ctx, filter, pipe)
	if err != nil {
		panic(err)
	}

	countRows := res.ModifiedCount

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted nullable column in %s to %d rows", elapsed, countRows)
	log.Print("==============================")
}

func addNullableWithDefault(client *mongo.Client, ctx context.Context) {
	start := time.Now()
	log.Print("======= ADD COLUMN WITH DEFAULT =======")
	log.Printf("Insert new column with default value in progress...")

	collection := client.Database("test").Collection("users")

	filter := bson.D{{}}
	pipe := bson.D{{"$set", bson.M{"default_column": "default text in new column"}}}
	res, err := collection.UpdateMany(ctx, filter, pipe)
	if err != nil {
		panic(err)
	}

	countRows := res.ModifiedCount

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Inserted new column with default value in %s to %d rows", elapsed, countRows)
	log.Print("==============================")
}

func dropColumn(client *mongo.Client, ctx context.Context) {
	start := time.Now()
	log.Print("======= DROP COLUMN =======")
	log.Printf("Drop column in progress...")

	collection := client.Database("test").Collection("users")

	filter := bson.D{{}}
	pipe := bson.D{{"$unset", bson.M{"default_column": ""}}}
	res, err := collection.UpdateMany(ctx, filter, pipe)
	if err != nil {
		panic(err)
	}

	countRows := res.ModifiedCount

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Drop column with default value in %s to %d rows", elapsed, countRows)
	log.Print("==============================")
}

func bulkCopy(client *mongo.Client, ctx context.Context) {
	if isUseTestSchema == true {
		amount = 1000000
	}
	countInWorker = int(amount / poolCount)

	start := time.Now()
	log.Print("========== BULK INSERT ARTICLES ============")
	log.Printf("Bulk insert %d articles in progress...", amount)

	var models []mog.WriteModel

	collection := client.Database("test").Collection("articles")
	opts := options.BulkWrite().SetOrdered(false)
	var objectID primitive.ObjectID
	var err error

	for i := 0; i < amount; i++ {
		title := fmt.Sprint("article_", i)

		authorId := int(i / 1000)

		objectID, err = primitive.ObjectIDFromHex(usersIdContainer.GetByKey(authorId))
		if err != nil {
			panic(err)
		}

		models = append(models, mog.NewInsertOneModel().SetDocument(&Article{
			ID:          primitive.NewObjectID(),
			AuthorId:    objectID,
			Title:       title,
			Description: "text",
		}))
	}

	res, err := collection.BulkWrite(ctx, models, opts)
	if err != nil {
		panic(err)
	}

	countRows := res.InsertedCount

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Bulk inserted %d rows in %s", countRows, elapsed)
	log.Print("==============================")
}
