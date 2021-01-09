package com.camillepradel.movierecommender.model.db;

import com.mongodb.BasicDBList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import com.mongodb.MongoCredential;
import com.mongodb.MongoClientOptions;

import org.bson.BasicBSONObject;
import org.bson.Document;
import org.json.JSONObject;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;

import java.rmi.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
public class MongodbDatabase extends AbstractDatabase {
	
	MongoClient mongoClient = null;
	DBCollection movies_coll = null;
	DBCollection users_coll = null;
	DB db = null;
	
    // db connection info
    String url = "mongodb://root:root@51.210.105.239:27017/?authSource=admin&authMechanism=SCRAM-SHA-1";
    String login = "root";
    String password = "root";

	public MongodbDatabase() {
        // load JDBC driver
    	mongoClient = new MongoClient(new MongoClientURI(url));
    	db = mongoClient.getDB("MovieLens");
		movies_coll = db.getCollection("movies");
		users_coll = db.getCollection("users");
    }
    
    @Override
    public List<Movie> getAllMovies() {
        // Return all movies from DB
        List<Movie> m = new LinkedList<Movie>();
        DBCursor cursor = movies_coll.find();
        for (DBObject movies : cursor) {
    		List<Genre> genres = new ArrayList<Genre>();
    		String[] listGenre = movies.get("genres").toString().split("\\|");
    		for (String genre : listGenre)
    		{
    			genres.add(new Genre(0, genre.toString()));
    		}
    		
    		m.add(new Movie((Integer)movies.get("_id"), (String)movies.get("title"), genres));
    	}
        return m;
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
    	// TODO: write query to retrieve all movies rated by user with id userId
    	List<Movie> m = new LinkedList<Movie>();
        DBCursor cursor = users_coll.find(new BasicDBObject("_id", userId));
        for (DBObject movies : cursor) {
        	//id_.add((Integer) result.get("_id"));
			List<BasicDBObject> listmovies = (List<BasicDBObject>) movies.get("movies");
			for (BasicDBObject movie : listmovies) {
        		int id = movie.getInt("movieid");
                DBCursor cursor2 = movies_coll.find(new BasicDBObject("_id", id));
                for (DBObject result : cursor2) {
            		List<Genre> genres = new ArrayList<Genre>();
            		String[] listGenre = result.get("genres").toString().split("\\|");
            		for (String genre : listGenre)
            		{
            			genres.add(new Genre(0, genre.toString()));
            		}
            		
            		m.add(new Movie((Integer)result.get("_id"), (String)result.get("title"), genres));
            	}
        	};
        };

        return m;
    }
    
    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        // TODO: write query to retrieve all ratings from user with id userId
    	List<Rating> ratings = new LinkedList<Rating>();
    	DBCursor cursor = users_coll.find(new BasicDBObject("_id", userId));
        for (DBObject movies : cursor) {
        	//id_.add((Integer) result.get("_id"));
        	List<BasicDBObject> listmovies = (List<BasicDBObject>) movies.get("movies");
			for (BasicDBObject movie : listmovies) {
        		int id = movie.getInt("movieid");
        		int rating = movie.getInt("rating");
                DBCursor cursor2 = movies_coll.find(new BasicDBObject("_id", id));
                for (DBObject result : cursor2) {
            		List<Genre> genres = new ArrayList<Genre>();
            		String[] listGenre = result.get("genres").toString().split("\\|");
            		for (String genre : listGenre)
            		{
            			genres.add(new Genre(0, genre.toString()));
            		}
            		
            		ratings.add(new Rating(new Movie((Integer)result.get("_id"), (String)result.get("title"), genres), userId, rating ));
            	}
        	};
        };
        return ratings;
        /*List<Rating> ratings = new LinkedList<Rating>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        ratings.add(new Rating(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 3));
        ratings.add(new Rating(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        return ratings;*/
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
    	int idMovie = rating.getMovieId();
    	int userId = rating.getUserId();
    	int score = rating.getScore();
    	
    	
    	/*DBCursor cursor = users_coll.find(new BasicDBObject("_id", userId));
        if (cursor.length() > 0) {
        	BasicDBObject updateQuery = new BasicDBObject("_id", userId);
        	updateQuery.put("movies.movieid", idMovie);
        	BasicDBObject updateCommand = new BasicDBObject("$set", new BasicDBObject("movies.$.rating", score));
        	users_coll.update(updateQuery, updateCommand);
        }else
        {*/
    	BasicDBObject docToInsert = new BasicDBObject("movieid", idMovie);
    	docToInsert.put("rating", score);
    	Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    	docToInsert.put("timestamp", timestamp);
    	BasicDBObject updateQuery = new BasicDBObject("_id", userId);
    	BasicDBObject updateCommand = new BasicDBObject("$push", new BasicDBObject("movies", docToInsert));
    	users_coll.update(updateQuery, updateCommand);
        //}

    	
    	
    	/*DBCursor cursor = users_coll.find(new BasicDBObject("_id", userId));
        for (DBObject movies : cursor) {
        	//id_.add((Integer) result.get("_id"));
        	List<BasicDBObject> new_movie = new ArrayList<BasicDBObject>();
        	List<BasicDBObject> listmovies = (List<BasicDBObject>) movies.get("movies");
			for (BasicDBObject movie : listmovies) {
        		int id = movie.getInt("movieid");
        		if (id == idMovie) {
        			
        			BasicDBObject docToInsert = new BasicDBObject("movieId", idMovie);
        			docToInsert.put("resourceName", "Foo Test3");
        			
        			BasicDBObject docToInsert = new BasicDBObject("resourceID", "3");
        			docToInsert.put("resourceName", "Foo Test3");
        			
        			BasicDBObject updateQuery = new BasicDBObject("_id", userId);
        			updateQuery.put("movies.movieId", id);
        			
        		    BasicDBObject query = new BasicDBObject();
        		    query.put("_id", id);
        		    query.put("movies."+idMovie, idMovie);

        		    BasicDBObject data = new BasicDBObject();
        		    data.put("movies.$.rating", rating.getScore());

        		    BasicDBObject command = new BasicDBObject();
        		    command.put("$set", data);
        		    users_coll.update(query, command);
        		}
			}
        		
        };*/
        
        // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        List<Rating> recommendations = new LinkedList<Rating>();
        String titlePrefix;
        if (processingMode == 0) {
            titlePrefix = "0_";
        } else if (processingMode == 1) {
            titlePrefix = "1_";
        } else if (processingMode == 2) {
            titlePrefix = "2_";
        } else {
            titlePrefix = "default_";
        }
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        return recommendations;
    }    
}