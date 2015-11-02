package org.apache.flink.examples.scala

import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.table._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object TableExperiments {

	def main(args: Array[String]): Unit = {
		val env = ExecutionEnvironment.createLocalEnvironment(1)
		// create a new temporary file, because we want to use Flink's csv reader
		val filePath = createTempFile(List("one,1", "two,2"))

		val inputSchema = Schema(Field("word", "String"), Field("nr", "int"))
		val source = env.readCsvFile(filePath)(ClassTag(classOf[Row]), inputSchema)
		println(source.getType())

		val transformations = List(new AddTimestampTransformation(), new MultipyTransformation("nr", 5))

		var transformed = source
		var schema = inputSchema
		for (t <- transformations) {
			transformed = t.transform(transformed)(schema)
			schema = t.createOutputSchema(schema)
		}
		println(transformed.getType())

		val sorted = transformed.sortPartition("nr", Order.DESCENDING)
		println(sorted.getType())

		val result = sorted.collect()
		println(result)
	}

	def createTempFile(lines: List[String]) = {
		val tempFile: File = File.createTempFile("table_test", "csv")
		Files.write(tempFile.toPath, lines.asJava, Charset.defaultCharset())
		tempFile.getAbsolutePath
	}
}

abstract class Transformation extends Serializable {

	protected implicit def generateOutputSchema(implicit inputSchema: Schema): TypeInformation[Row] =
		createOutputSchema(inputSchema)

	def createOutputSchema(inputSchema: Schema): Schema

	def transform(input: DataSet[Row])(implicit inputSchema: Schema): DataSet[Row]

}

class AddTimestampTransformation extends Transformation {

	override def createOutputSchema(inputSchema: Schema): Schema =
		inputSchema.add(Field("timestamp", "long"))

	override def transform(input: DataSet[Row])(implicit is: Schema): DataSet[Row] = {
		input.map(row => {
			new Row(row, System.currentTimeMillis())
		})
	}
}

class MultipyTransformation(val targetFieldName: String, val factor: Int)
				extends Transformation {

	override def createOutputSchema(inputSchema: Schema): Schema = inputSchema

	override def transform(input: DataSet[Row])(implicit schema: Schema): DataSet[Row] = {
		input.map(row => {
			val copy = new Row(row)
			copy(targetFieldName) = row.getAs[Int](targetFieldName) * factor
			copy
		})
	}
}
