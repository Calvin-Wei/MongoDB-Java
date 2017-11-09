package com.mongodb.exercise;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class MongoDBJDBC {
	// 连接MongoDB数据库无需认证
	public static void noAuthorise() {
		MongoClient mongoClient = null;
		try {
			// 连接到mongodb服务
			mongoClient = new MongoClient("localhost", 27017);
			// 连接数据库名称为col的
			MongoDatabase mongoDatabase = mongoClient.getDatabase("col");
			System.out.println("Connect to database successfully");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (mongoClient != null)
				mongoClient.close();
		}
	}

	// 连接MongoDB数据库需要认证条件
	public static void connServer() {
		MongoClient mongoClient = null;
		try {
			// 连接到Mongodb服务
			ServerAddress serverAddress = new ServerAddress("localhost", 27017);
			List<ServerAddress> addrs = new ArrayList<>();
			addrs.add(serverAddress);
			// MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
			MongoCredential credential = MongoCredential.createScramSha1Credential("username", "databaseName",
					"password".toCharArray());
			List<MongoCredential> credentials = new ArrayList<MongoCredential>();
			credentials.add(credential);
			// 通过连接认证获取MongoDB连接
			mongoClient = new MongoClient(addrs, credentials);
			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("databaseName");
			System.out.println("Connect to database successfully!");
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} finally {
			if (mongoClient != null)
				mongoClient.close();
		}

	}

	// 创建集合
	public static void createCollection() {
		MongoClient mongoClient = null;
		try {
			// 连接到mongodb服务
			mongoClient = new MongoClient("localhost", 27017);
			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("col");
			System.out.println("Connect to database successfully");
			mongoDatabase.createCollection("test");
			System.out.println("集合创建成功");
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} finally {
			if (mongoClient != null)
				mongoClient.close();
		}
	}

	// 插入文档数据
	public static void insertMany() {
		MongoClient mongoClient = null;
		try {
			// 连接到mongodb服务
			mongoClient = new MongoClient("localhost", 27017);
			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("col");
			System.out.println("Connect to database successfully.");
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");
			System.out.println("集合test选择成功!");
			// 插入文档
			// 1.创建文档org.bson.Document参数为key-value的格式
			// 2.创建文档集合List<Document>
			// 3.将文档集合插入数据库集合中mongoCollection.insertMany(List<Document>)
			// 插入文档可以用mongoCollection.insertOne(Document)
			Document document = new Document("title", "MongoDB").append("description", "database").append("likes", 100)
					.append("by", "Fly");
			List<Document> documents = new ArrayList<>();
			documents.add(document);
			collection.insertMany(documents);
			System.out.println("文档插入成功");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (mongoClient != null)
				mongoClient.close();
		}
	}
	//查询文档
	public static void queryDocument() {
		MongoClient mongoClient = null;
		try {
			// 连接到mongodb服务
			mongoClient = new MongoClient();
			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("col");
			System.out.println("Connect to database successfully");
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");
			System.out.println("选择集合test成功");
			// 检索所有文档
			// 1.获取迭代期FindIterable<Document>
			// 2.获取游标MongoCursor<Document>
			// 3.通过游标遍历检索出的文档集合
			FindIterable<Document> findIterable = collection.find();
			MongoCursor<Document> mongoCursor = findIterable.iterator();
			while (mongoCursor.hasNext()) {
				System.out.println(mongoCursor.next());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (mongoClient != null)
				mongoClient.close();
		}
	}

	// 批量更新文档
	public static void updateMany() {
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase mongoDatabase = mongoClient.getDatabase("col");
			System.out.println("Connect to database successfully");
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");
			System.out.println("集合test选择成功");
			// 批量更新文档，将文档中likes=100文档修改为likes=200
			collection.updateMany(Filters.eq("likes", 100), new Document("$set", new Document("likes", 200)));
			// 检索查看结果
			FindIterable<Document> findIterable = collection.find();
			MongoCursor<Document> mongoCursor = findIterable.iterator();
			while (mongoCursor.hasNext()) {
				System.out.println(mongoCursor.next());
			}
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} finally {
			if (mongoClient != null)
				mongoClient.close();
		}
	}
	//删除第一个文档
	public static void deleteOne() {
		MongoClient mongoClient=null;
		try {
			mongoClient=new MongoClient("localhost",27017);
			MongoDatabase mongoDatabase=mongoClient.getDatabase("col");
			System.out.println("Connect to database successfully!");
			MongoCollection<Document> collection=mongoDatabase.getCollection("test");
			System.out.println("集合test选择成功");
			//删除符合条件的第一个文档
			collection.deleteOne(Filters.eq("likes", 200));
			//删除所有符合条件的文档
			collection.deleteMany(Filters.eq("likes", 200));
			//检索查看结果
			FindIterable<Document> findIterable=collection.find();
			MongoCursor<Document> mongoCursor=findIterable.iterator();
			while(mongoCursor.hasNext()) {
				System.out.println(mongoCursor.next());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(mongoClient!=null)
				mongoClient.close();
		}
	}
	
	public static void main(String[] args) {
		deleteOne();
	}
}
