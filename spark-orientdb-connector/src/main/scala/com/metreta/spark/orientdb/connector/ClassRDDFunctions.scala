
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.orientechnologies.orient.core.sql.OCommandSQL
import org.apache.spark.Logging
import java.text.SimpleDateFormat
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import java.lang.reflect.Field
import com.orientechnologies.orient.core.metadata.schema.OProperty
import org.apache.commons.codec.binary.Base64
import java.math.BigDecimal
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.TaskContext
import com.metreta.spark.orientdb.connector.rdd.OrientDocumentException

class ClassRDDFunctions[T](rdd: RDD[T]) extends Serializable with Logging {
  /**
   * Saves an instance of [[org.apache.spark.rdd.RDD RDD]] into an OrientDB class.
   * The Input class must have been created on OrientDB, the rdd must be composed by instances of a
   * case class or of primitive objects and case class can have a number of attribute >= 0.
   *
   * @param orientClass
   */
  def saveToOrient(orientClass: String)(implicit connector: OrientDBConnector = OrientDBConnector(rdd.sparkContext.getConf)): Unit = {

    rdd.foreachPartition { partition =>
      val db = connector.databaseDocumentTx()

      while (partition.hasNext) {
        val obj = partition.next()
        val doc = new ODocument(orientClass);
        setProperties("value", doc, obj)
        db.save(doc)

      }
      db.commit()
      db.close()
    }

  }

  /**
   * Upserts an instance of [[org.apache.spark.rdd.RDD RDD]] into an OrientDB class.
   * @param orientClass
   * @params keyColumns
   * @params where
   */

  def upsertToOrient(orientClass: String, keyColumns: List[String], where: String = "")(implicit connector: OrientDBConnector = OrientDBConnector(rdd.sparkContext.getConf)): Unit = {

    if (keyColumns.size < 1)
      throw new OrientDocumentException(s"Please specify at leas one key column for ${orientClass} class.")

    rdd.foreachPartition { partition =>
      val db = connector.databaseDocumentTx()

      while (partition.hasNext) {
        val obj = partition.next()

        var fromQuery = s"UPDATE ${orientClass} ${getInsertString(obj)} upsert return after @rid where ${getUpsertWhereString(obj, keyColumns)}"
        if (!where.isEmpty()) fromQuery = fromQuery + s" AND ${where}"

        try {
          db.command(new OCommandSQL(fromQuery)).execute().asInstanceOf[java.util.ArrayList[Any]]
        } catch {
          case e: Exception => {
            db.rollback()
            e.printStackTrace()
          }
        }
      }
      db.commit()
      db.close()
    }

  }

  /**
   * Converts an instance of a case class to a string which will
   * be utilized for SQL INSERT command composition.
   *
   * Example:
   * given a case class Person(name: String, surname: String)
   *
   * getInsertString(Person("Larry", "Page")) will return a String: " name = 'Larry', surname = 'Page'"
   *
   * @param orientClass used to obtain the fields types
   * @param obj an object
   * @return a string
   */

  private def getInsertString[T](obj: T): String = {

    var insStr = "SET"
    if (obj != null) {
      obj match {
        case o: Int            => insStr = insStr + " value = " + o + ","
        case o: Boolean        => insStr = insStr + " value = " + o + ","
        case o: BigDecimal     => insStr = insStr + " value = " + o + ","
        case o: Float          => insStr = insStr + " value = " + o + ","
        case o: Double         => insStr = insStr + " value = " + o + ","
        case o: java.util.Date => insStr = insStr + " value = date('" + orientDateFormat.format(o) + "'),"
        case o: Short          => insStr = insStr + " value = " + o + ","
        case o: Long           => insStr = insStr + " value = " + o + ","
        case o: String         => insStr = insStr + " value = '" + escapeString(o) + "',"
        case o: Array[Byte]    => insStr = insStr + " value = '" + Base64.encodeBase64String(o.asInstanceOf[Array[Byte]]) + "',"
        case o: Byte           => insStr = insStr + " value = " + o + ","

        case o => {
          obj.getClass().getDeclaredFields.foreach {
            case field =>
              field.setAccessible(true)
              insStr = insStr + " " + field.getName + " = " + buildValueByType(field.get(obj)) + ","
          }
        }
      }
    }
    insStr.dropRight(1)
  }

  private def getUpsertWhereString[T](obj: T, keyColumns: List[String]): String = {

    var upsStr = ""
    for (key <- keyColumns) {
      val field = obj.getClass().getDeclaredField(key)
      field.setAccessible(true)
      val value = field.get(obj)
      upsStr = upsStr + " " + key + " = " + buildValueByType(value) + " AND"
    }

    upsStr.dropRight(3)
  }

  private val orientDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private def buildValueByType(fieldValue: AnyRef): String = fieldValue match {
    case _: Array[Byte]      => "'" + Base64.encodeBase64String(fieldValue.asInstanceOf[Array[Byte]]) + "'" //"'" + (fieldValue.asInstanceOf[Array[Byte]]) + "'"//
    case _: java.lang.String => "'" + escapeString(fieldValue.toString) + "'"
    case _: java.util.Date   => "date('" + orientDateFormat.format(fieldValue) + "')"
    case _                   => fieldValue.toString()
  }

  private def setProperties[T](fieldName: String, doc: ODocument, obj: T): Unit = {
    if (obj != null) {
      obj match {
        case o: Int            => doc.field(fieldName, o, OType.INTEGER)
        case o: Boolean        => doc.field(fieldName, o, OType.BOOLEAN)
        case o: BigDecimal     => doc.field(fieldName, o, OType.DECIMAL)
        case o: Float          => doc.field(fieldName, o, OType.FLOAT)
        case o: Double         => doc.field(fieldName, o, OType.DOUBLE)
        case o: java.util.Date => doc.field(fieldName, orientDateFormat.format(o), OType.DATE)
        case o: Short          => doc.field(fieldName, o, OType.SHORT)
        case o: Long           => doc.field(fieldName, o, OType.LONG)
        case o: String         => doc.field(fieldName, o, OType.STRING)
        case o: Array[Byte]    => doc.field(fieldName, o, OType.BINARY) //insStr = insStr + " value = '" + Base64.encodeBase64String(o.asInstanceOf[Array[Byte]]) + "',"  
        case o: Byte           => doc.field(fieldName, o, OType.BYTE)
        case _ => {
          obj.getClass().getDeclaredFields.foreach {
            case field =>
              field.setAccessible(true)
              setProperties(field.getName, doc, field.get(obj))
          }
        }
      }
    }
  }

  private def escapeString(in: String): String = {

    return in.replace("'", "\\'").replace("\"", "\\\"")
  }

}