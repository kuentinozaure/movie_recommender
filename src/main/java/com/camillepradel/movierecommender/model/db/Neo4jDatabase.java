package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.spi.Connection;

public class Neo4jDatabase extends AbstractDatabase implements AutoCloseable {

     private Driver driver;

    // db connection info
    String url = "bolt://localhost:7687";
    String login = "neo4j";
    String password = "neo4j";
    
    public Neo4jDatabase() {

       try {
          this.driver  =  GraphDatabase.driver( url, AuthTokens.basic( login, password ) );
       } catch (Exception e) {
           e.printStackTrace();
       }
    }
    
    @Override
    public List<Movie> getAllMovies() {
        // TODO: write query to retrieve all movies from DB

        String matchQuery = "MATCH (m:Movie)-[:CATEGORIZED_AS]->(g:Genre) RETURN m, collect(g) as genres";
        try (Session session = driver.session()) {
            return session.run(matchQuery).list(r
                    -> new Movie(r.get(0).get("id").asInt(),
                            r.get(0).get("title").asString(),
                            r.get(1).asList(value
                                    -> new Genre(value.get("id").asInt(),
                                    value.get("name").asString()))));
        }

    }
    

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                List<Movie> movies = new LinkedList<>();
                Result result = tx.run(
                        "MATCH (u:User{id:" + Integer.toString(userId) + "})"
                        + "-[:RATED]->(m:Movie)"
                        + "-[:CATEGORIZED_AS]->(g:Genre) RETURN m, collect(g)");
                while (result.hasNext()) {
                    Record row = result.next();
                    int movieId = row.get(0).get("id").asInt();
                    String movieTitle = row.get(0).get("title").asString();
                    Iterable<Value> genres = row.get(1).values();
                    List<Genre> movieGenres = new LinkedList<>();
                    genres.forEach(genre -> {
                        movieGenres.add(new Genre(genre.get("id").asInt(), genre.get("name").asString()));
                    });
                    movies.add(new Movie(movieId, movieTitle, movieGenres));
                }
                return movies;
            });
        }
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        // TODO: write query to retrieve all ratings from user with id userId
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                List<Rating> ratings = new LinkedList<>();
                Result result = tx.run(
                        "MATCH (u:User{id:" + Integer.toString(userId) + "})"
                        + "-[r:RATED]->(m:Movie)"
                        + "-[:CATEGORIZED_AS]->(g:Genre) RETURN u, r, m, collect(g)");
                while (result.hasNext()) {
                    Record row = result.next();
                    // Information from user
                    int uId = row.get(0).get("id").asInt();
                    // Information from rated
                    int note = row.get(1).get("note").asInt();
                    // Information from movie
                    int mId = row.get(2).get("id").asInt();
                    String movieTitle = row.get(2).get("title").asString();
                    // Information from genre
                    Iterable<Value> genres = row.get(3).values();
                    List<Genre> movieGenres = new LinkedList<>();
                    genres.forEach(
                            genre -> {
                                movieGenres.add(new Genre(genre.get("id").asInt(), genre.get("name").asString()));
                            }
                    );
                    // Create Movie
                    Movie movie = new Movie(mId, movieTitle, movieGenres);
                    // Create ratings
                    ratings.add(
                            new Rating(
                                    movie,
                                    uId,
                                    note
                            )
                    );
                }
                return ratings;
            });
        }
    }
    @Override
    public void addOrUpdateRating(Rating rating) {
        // EXAMPLE TRUE : RETURN EXISTS( (:User{id:8})-[:RATED]->(:Movie{id:7}) )
        // EXAMPLE FALSE : RETURN EXISTS( (:User{id:8})-[:RATED]->(:Movie{id:1}) )
        int movieId = rating.getMovieId();
        int userId = rating.getUserId();
        int note = rating.getScore();
        try (Session session = driver.session()) {
            session.writeTransaction(new TransactionWork<Integer>() {
                @Override
                public Integer execute(Transaction tx) {
                    Boolean isAlreadyExists = isRatingAlreadyExists(userId, movieId);
                    if (isAlreadyExists) {
                        tx.run("MATCH (:User {id:" + Integer.toString(userId) + "})"
                                + "-[r:RATED]->(:Movie {id:" + Integer.toString(movieId) + "}) "
                                + "SET r.note = " + note);
                    } else {
                        Date date = new Date();
                        tx.run("MATCH (u:User {id:" + Integer.toString(userId) + "}),"
                                + "(m:Movie {id:" + Integer.toString(movieId) + "}) "
                                + "CREATE (u)-[r:RATED { note: " + note + ", "
                                + "timestamp: " + date.getTime() + "}]->(m) ");
                    }
                    return 1;
                }
            });
        }
    }
    
    public Boolean isRatingAlreadyExists(int userId, int movieId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run(
                        "RETURN EXISTS( (:User{id:" + Integer.toString(userId) + "})"
                                + "-[:RATED]->(:Movie{id:" + Integer.toString(movieId) + "}) )");
                return result.next().get(0).asBoolean();
            });
        }
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        List<Rating> recommendations = new LinkedList<>();
        String titlePrefix;
        switch (processingMode) {
            case 0:
                titlePrefix = "0_";
                break;
            case 1:
                titlePrefix = "1_";
                break;
            case 2:
                titlePrefix = "2_";
                break;
            default:
                titlePrefix = "default_";
                break;
        }
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        return recommendations;
    }

    @Override
    public void close() throws Exception {
       driver.close();    }
}
