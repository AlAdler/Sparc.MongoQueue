namespace Sparc.MongoQueue
{
    using MongoDB.Bson;
    using MongoDB.Driver;
    using MongoDB.Driver.Builders;
    using System;

    public class MongoQueue
    {
        private MongoDatabase db;

        public string QueueName { get; private set; }

        public string CollectionName { get; set; }

        public MongoQueue(MongoDatabase db, string queueName)
        {
            if (string.IsNullOrWhiteSpace(queueName))
            {
                throw new ArgumentNullException("queueName");
            }

            if (db == null)
            {
                throw new ArgumentNullException("db");
            }

            this.db = db;
            this.QueueName = queueName;
            this.CollectionName = "MongoQueue";
        }

        public void Push(BsonDocument data, Schedule schedule = null)
        {
            if (data == null)
            {
                throw new ArgumentNullException("data");
            }

            schedule = schedule ?? new Schedule { Repeat = Repeat.None, NextRun = DateTime.UtcNow };
            var item = new BsonDocument();
            var meta = new BsonDocument();
            meta["QueueName"] = this.QueueName;
            item["MongoQueue"] = meta;
            item["data"] = data;
            this.db.GetCollection(this.CollectionName).Save(item);
        }

        public BsonDocument Pop()
        {
            var collection = this.db.GetCollection(this.CollectionName);
            var query = Query.EQ("MongoQueue.QueueName", this.QueueName);
            var resultd = collection.FindAndRemove(
                query,
                SortBy.Null);

            var result = resultd.ModifiedDocument;
            if (result != null)
            {
                result = result["data"].AsBsonDocument;
            }
            return result;
        }
    }
}
