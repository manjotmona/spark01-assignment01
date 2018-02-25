package edu.knoldus

import org.apache.spark.sql.SparkSession


/**
 * Created by pallavi on 24/2/18.
 */


case class Customer(CustomerId: Int,
    CustomerName: String,
    Street: String,
    City: String,
    State: String,
    Zip: String)

case class Transaction(Timestamp: Long, CustomerId: Int, price: Int)

case class CustomerResult(CustomerId: Int, City: String)

object SparkGetTotalPrice extends App {


  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Get Price")
    .getOrCreate()


  val transactionRdd = spark.sparkContext.textFile("/home/pallavi/Desktop/inputFile2.txt")

  val customerRdd = spark.sparkContext.textFile("/home/pallavi/Desktop/inputFile1.txt")

  val transactionObjectsArray = transactionRdd.map(x => x.split("#"))
    .map(y => Transaction(y(0).toLong, y(1).toInt, y(2).toInt))

  val customerObjectsArray = customerRdd.map(line => line.split("#"))
    .map(line => Customer(line(0).toInt,
      line(1).toString,
      line(2).toString,
      line(3).toString,
      line(4).toString,
      line(5).toString))
  val customerIdAndCityObjectArray = customerObjectsArray.map(x => (x.CustomerId, x.State))
  val kvTransactions = transactionObjectsArray.map(x => (x.CustomerId, x))
  val kvCustomers = customerObjectsArray.map(x => (x.CustomerId, x))

  val joinResult = customerIdAndCityObjectArray.join(kvTransactions)

  val rddForQuerying = joinResult.map(x => (x._1, x._2._2.Timestamp, x._2._1, x._2._2.price))

  val yearlyRdd = rddForQuerying
    .map(x => ((x._1, new org.joda.time.DateTime(x._2 * 1000).getYear, x._3), x._4))

  val yearlyOutput = yearlyRdd.reduceByKey((a, b) => a + b)
  val formatedYearly = yearlyOutput.map(x => s"${ x._1._3 }#${ x._1._2 }###${ x._2 }")


  yearlyOutput.foreach(x => println(x))

  val yearlyAndMonthlyRdd = rddForQuerying
    .map(x => ((x._1, new org.joda.time.DateTime(x._2 * 1000).getYear, new org.joda.time.DateTime(
      x._2 * 1000).getMonthOfYear, x._3), x._4))

  val monthlyOutput = yearlyAndMonthlyRdd.reduceByKey((a, b) => a + b)
  val formatedMonthly = monthlyOutput.map(x => s"${ x._1._4 }#${ x._1._2 }#${ x._1._3 }##${ x._2 }")

  monthlyOutput.foreach(x => println(x))


  val dailyRdd = rddForQuerying
    .map(x => ((x._1, new org.joda.time.DateTime(x._2 * 1000).getYear, new org.joda.time.DateTime(
      x._2 * 1000).getMonthOfYear, new org.joda.time.DateTime(x._2 * 1000).getDayOfMonth, x._3), x
      ._4))
  val dailyOutput = dailyRdd.reduceByKey((a, b) => a + b)
  val formatedDaily = dailyOutput
    .map(x => s"${ x._1._5 }#${ x._1._2 }#${ x._1._3 }#${ x._1._4 }#${ x._2 }")


  dailyOutput.foreach(x => println(x))

  val mergeRdd = formatedMonthly.union(formatedDaily).union(formatedYearly)

  mergeRdd.repartition(1).saveAsTextFile("/home/pallavi/Desktop/outputFile.txt")
}