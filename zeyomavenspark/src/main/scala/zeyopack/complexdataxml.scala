package zeyopack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexdataxml {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("complexdata").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val data = spark.read.format("com.databricks.spark.xml").option("rowTag", "POSLog").load("file:///F:/Big Data/Complex Data/transactions.xml")

    data.show()
    data.printSchema()

    val flattendata = data.withColumn("Transaction", expr("explode(Transaction)"))
      .withColumn("LineItem", expr("explode(Transaction.RetailTransaction.LineItem)"))
      .withColumn("Total", expr("explode(Transaction.RetailTransaction.Total)"))
      .select(

        col("Transaction.BusinessDayDate"),
        col("Transaction.ControlTransaction.OperatorSignOff.*"),
        col("Transaction.ControlTransaction.ReasonCode"),
        col("Transaction.ControlTransaction._Version"),
        col("Transaction.CurrencyCode"),
        col("Transaction.EndDateTime"),
        col("Transaction.OperatorID.*"),
        col("Transaction.RetailStoreID"),
        col("Transaction.RetailTransaction.ItemCount"),
        col("Transaction.RetailTransaction.PerformanceMetrics.*"),
        col("Transaction.RetailTransaction.ReceiptDateTime"),
        col("Transaction.RetailTransaction.TransactionCount"),
        col("Transaction.RetailTransaction._Version"),
        col("Transaction.SequenceNumber"),
        col("Transaction.WorkstationID"),
        col("LineItem.Sale.Description"),
        col("LineItem.Sale.DiscountAmount"),
        col("LineItem.Sale.ExtendedAmount"),
        col("LineItem.Sale.ExtendedDiscountAmount"),
        col("LineItem.Sale.ItemID"),
        col("LineItem.Sale.Itemizers.*"),
        col("LineItem.Sale.MerchandiseHierarchy.*"),
        col("LineItem.Sale.OperatorSequence"),
        col("LineItem.Sale.POSIdentity.*"),
        col("LineItem.Sale.Quantity"),
        col("LineItem.Sale.RegularSalesUnitPrice"),
        col("LineItem.Sale.ReportCode"),
        col("LineItem.Sale._ItemType"),
        col("LineItem.SequenceNumber"),
        col("LineItem.Tax.*"),
        col("LineItem.Tender.Amount"),
        col("LineItem.Tender.Authorization.*"),
        col("LineItem.Tender.OperatorSequence"),
        col("LineItem.Tender.TenderID"),
        col("LineItem.Tender._TenderDescription"),
        col("LineItem.Tender._TenderType"),
        col("LineItem.Tender._TypeCode"),
        col("LineItem._EntryMethod"),
        col("LineItem._weightItem"),
        col("Total._TotalType"),
        col("Total._VALUE"))

    flattendata.show()
    flattendata.printSchema()

  }
}