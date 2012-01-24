﻿namespace Sparc.MongoQueue
{
    using System;
    using MongoDB.Bson;
    using MongoDB.Driver;
    using MongoDB.Driver.Builders;

    /// <summary>
    /// Manages a queuing mechanism using MongoDB.
    /// </summary>
    public class MongoQueue
    {
        private MongoDatabase db;

        /// <summary>
        /// Initializes a new instance of the MongoQueue class.
        /// </summary>
        /// <param name="db">The mongo database to use.</param>
        /// <param name="queueName">The name of the queue to be managed.
        /// (e.g. "Notifications", "BatchJobs", etc)
        /// </param>
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
            this.MaxProcessingTime = TimeSpan.FromMinutes(30);
            this.CollectionName = "MongoQueue";
        }

        /// <summary>
        /// Gets the name of the queue being used.
        /// </summary>
        public string QueueName { get; private set; }

        /// <summary>
        /// Gets or sets the name of the Mongo collection to use.
        /// </summary>
        public string CollectionName { get; set; }

        /// <summary>
        /// Gets or sets the maximum processing time for queue items.
        /// The default is 30 minutes.  This should be a positive value.
        /// </summary>
        public TimeSpan MaxProcessingTime { get; set; }

        /// <summary>
        /// Pushes an item on to the queue.
        /// </summary>
        /// <param name="data">The item to be pushed.</param>
        public void Push(BsonDocument data)
        {
            if (data == null)
            {
                throw new ArgumentNullException("data");
            }

            db.GetCollection(CollectionName).Save(new BsonDocument
            {
                { "QueueName", this.QueueName },
                { "Data", data }
            });
        }

        /// <summary>
        /// Pops an item off of the queue.
        /// </summary>
        /// <returns>The item to process.</returns>
        public MongoQueueItem Pop()
        {
            var collection = db.GetCollection(CollectionName);
            var query = Query.And(
                Query.EQ("QueueName", this.QueueName),
                Query.Or(Query.Exists("Machine", false), Query.LT("Expires", DateTime.UtcNow)));
            var item = collection.FindAndModify(
                query,
                SortBy.Null,
                Update.Set("Machine", Environment.MachineName).Set("Expires", DateTime.UtcNow.Add(MaxProcessingTime))).ModifiedDocument;

            return item == null ? null : new MongoQueueItem(collection, item);
        }
    }
}