package com.meshal.first_project;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.RoutingContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


public class MainVerticle extends AbstractVerticle {
  private static final String SQL_GET_ALL_STAFF = "SELECT first_name, last_name, email FROM sakila.staff";
  private static final String SQL_INSERT_STAFF = "INSERT INTO sakila.staff(first_name,last_name)"; //back here to changes the query
  private static final String SQL_UPDATE_STAFF_INFO = "update sakila.staff set last_name = ? where staff_id =?";
  private static final String SQL_DELETE_STAFF = "DELETE FROM sakila.staff WHERE username=?"; //need to changes



  // Create a router object.

  @Override
  public void start(Future<Void> fut) {


    Router router = Router.router(vertx);

    // Bind "/" to our hello message - so we are still compatible.


    router.route().handler(BodyHandler.create());
    System.out.println("router");
    router
      .get("/api/staff").handler(this::handlerGetStaff);


    router
      .post().handler(BodyHandler.create()); // decodes the body from the HTTP requests
    router
      .post("/api/staff/update/staffId").handler(this::handlerUpdateStaff);
    router
      .post("/api/staff/create/:userName").handler(this::handlerAddStaff);
    router
      .post("/api/staff/delete/:staffId").handler(this::handlerDeleteStaff);



    // Create the HTTP server and pass the "accept" method to the request handler.
    vertx
      .createHttpServer()
      .requestHandler(router::accept)
      .listen(
        // Retrieve the port from the configuration,
        // default to 8080.
        config().getInteger("http.port", 8088),
        result -> {
          if (result.succeeded()) {
            fut.complete();
          } else {
            fut.fail(result.cause());
          }
        }
      );


  }

  // get all users
  public void handlerGetStaff(RoutingContext routingContext) {
    HttpServerResponse response = routingContext.response();

    System.out.println("inside handle ");

    MySQLConnectOptions connectOptions = new MySQLConnectOptions()


      .setPort(3306)
      .setHost("127.0.0.1")
      .setDatabase("sakila")
      .setUser("root")
      .setPassword("asd12345");

    // Pool options
    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(5);

    // Create the pooled client
    MySQLPool client = MySQLPool.pool(vertx, connectOptions, poolOptions);
    client.getConnection(ar1 -> {

      if (ar1.succeeded()) {

        System.out.println("Connected");

        // Obtain our connection
        SqlConnection conn = ar1.result();

        // All operations execute on the same connection
        conn.query(SQL_GET_ALL_STAFF, ar2 -> {

          conn.close();
          JsonArray arrAllStaff = new JsonArray();
          if (ar2.succeeded()) {

            RowSet<Row> allStaffRows = ar2.result();
            for (Row row : allStaffRows) {
              arrAllStaff.add(row.getString(0)+ " " + row.getString(1));
              System.out.println("User " + row.getString(0) + " " + row.getString(1));
              System.out.println("arrlistStaff" + arrAllStaff);
            }


            response
              .setStatusCode(200)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(Json.encodePrettily(ar2.result()));

          } else {
            // Release the connection to the pool
            conn.close();
            System.out.println("Sakila query not working");
          }

        });
      } else {
        System.out.println("Could not connect: " + ar1.cause().getMessage());
      }
    });

  }


  private void handlerAddStaff(RoutingContext routingContext) {
    String staffName = routingContext.request().getParam("userName");
    String location = "/api/staff/userName";
    HttpServerResponse response = routingContext.response();

    System.out.println("inside handle ");

    MySQLConnectOptions connectOptions = new MySQLConnectOptions()


      .setPort(3306)
      .setHost("127.0.0.1")
      .setDatabase("sakila")
      .setUser("root")
      .setPassword("asd12345");

    // Pool options
    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(5);

    // Create the pooled client
    MySQLPool client = MySQLPool.pool(vertx, connectOptions, poolOptions);
    client.getConnection(ar1 -> {

      if (ar1.succeeded()) {

        System.out.println("Connected");

        // Obtain our connection
        SqlConnection conn = ar1.result();

        // All operations execute on the same connection
        conn.query(SQL_INSERT_STAFF, ar2 -> {

          conn.close();
          JsonArray arrAllStaff = new JsonArray();
          if (ar2.succeeded()) {


            response
              .setStatusCode(303)
              .putHeader("Location",location)
              .end("<h1> Added staff successfully </h1>");

          } else {
            // Release the connection to the pool
            conn.close();
            System.out.println("Sakila query not working");
          }

        });
      } else {
        System.out.println("Could not connect: " + ar1.cause().getMessage());
      }
    });
  }


  private void handlerUpdateStaff(RoutingContext routingContext) {
    String staffId = routingContext.request().getParam(":staffId");
    String last_name = routingContext.request().getParam("first_name");
    System.out.println("user put id " + staffId);

    HttpServerResponse response = routingContext.response();

    MySQLConnectOptions connectOptions = new MySQLConnectOptions()


      .setPort(3306)
      .setHost("127.0.0.1")
      .setDatabase("sakila")
      .setUser("root")
      .setPassword("asd12345");

    // Pool options
    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(5);

    // Create the pooled client
    MySQLPool client = MySQLPool.pool(vertx, connectOptions, poolOptions);
    client.getConnection(ar1 -> {

      if (ar1.succeeded()) {

        System.out.println("Connected");

        // Obtain our connection
        SqlConnection conn = ar1.result();

        String sql = SQL_UPDATE_STAFF_INFO;
        JsonArray params = new JsonArray();
        params.add(last_name);
        // All operations execute on the same connection
        conn.preparedQuery(sql, ar2 -> {
          conn.close();

          if (ar2.succeeded()) {
            response
              .setStatusCode(303)
              .putHeader("Location", "/api/staff")
              .end("<h1> update staff successfully </h1>");

          } else {
            // Release the connection to the pool
            conn.close();
            System.out.println("Sakila query not working");
          }

        });
      } else {
        System.out.println("Could not connect: " + ar1.cause().getMessage());
      }


    });
  }



  private void handlerDeleteStaff(RoutingContext routingContext) {
    String username = routingContext.request().getParam("username");
    System.out.println("user delete id " + username);

    HttpServerResponse response = routingContext.response();

    System.out.println("inside handle ");

    MySQLConnectOptions connectOptions = new MySQLConnectOptions()


      .setPort(3306)
      .setHost("127.0.0.1")
      .setDatabase("sakila")
      .setUser("root")
      .setPassword("asd12345");

    // Pool options
    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(5);

    // Create the pooled client
    MySQLPool client = MySQLPool.pool(vertx, connectOptions, poolOptions);
    client.getConnection(ar1 -> {

      if (ar1.succeeded()) {

        System.out.println("Connected");

        // Obtain our connection
        SqlConnection conn = ar1.result();

        // All operations execute on the same connection
        conn.query(SQL_DELETE_STAFF, ar2 -> {
          conn.close();

          if (ar2.succeeded()) {
            response
              .setStatusCode(303)
              .putHeader("Location","/")
              .end("<h1> delete staff successfully </h1>");

          } else {
            // Release the connection to the pool
            conn.close();
            System.out.println("Sakila query not working");
          }

        });
      } else {
        System.out.println("Could not connect: " + ar1.cause().getMessage());
      }

      });
  }

}

