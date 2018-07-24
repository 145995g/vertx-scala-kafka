package com.unisys.os2200

import com.typesafe.scalalogging
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.ext.web.Router
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import kafka.utils.json.JsonObject

import scala.concurrent.Future

class HttpVerticle extends ScalaVerticle {

  val logger = scalalogging.Logger(classOf[HttpVerticle])

  override def startFuture(): Future[_] = {

    //Initializing the kafka proucer.
    //var config = Map()
    //var config = Map()
    //USTR-ERL2-3508.na.uis.unisys.com
    var kafkaConfig = scala.collection.mutable.Map[String, String]()
    kafkaConfig += ("bootstrap.servers" -> "localhost:9092")
    kafkaConfig += ("key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")
    kafkaConfig += ("value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")
    kafkaConfig += ("acks" -> "1")
    logger.debug(s"Kafka Producer Configuration :$config")

    // use producer for interacting with Apache Kafka
    //var consumer: KafkaConsumer[String, String]
    var producer = KafkaProducer.create(vertx,kafkaConfig)


    //Create a router to answer GET-requests to "/hello" with "world"
    val router = Router.router(vertx)
    val route = router
      .post("/account-transfer")
      .handler((routingContext: io.vertx.scala.ext.web.RoutingContext) => {
        var requestBody =  routingContext.getBodyAsJson()
        var response = routingContext.response()
        response.putHeader("content-type", "application/json")
        response.end("{'status':'success','desc':'Transfer initiated'}")

        var record :KafkaProducerRecord[Nothing,Nothing]= KafkaProducerRecord.

        producer.write(record)


      })

    vertx
      .createHttpServer()
      .requestHandler(router.accept _)
      .listenFuture(8666, "0.0.0.0")
  }



}
