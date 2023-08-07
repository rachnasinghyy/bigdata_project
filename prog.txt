Project Whole Code

package pack

import java.security.cert.X509Certificate
import javax.net.ssl._
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj {

	def main(args: Array[String]): Unit = {


			val sslContext = SSLContext.getInstance("TLS")
					sslContext.init(null, Array(new X509TrustManager {
						override def getAcceptedIssuers: Array[X509Certificate] = Array.empty[X509Certificate]
								override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
					override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
					}), new java.security.SecureRandom())
					val hostnameVerifier = new HostnameVerifier {override def verify(s: String, sslSession: SSLSession): Boolean = true}

			val httpClient = HttpClients.custom().setSSLContext(sslContext).setSSLHostnameVerifier(hostnameVerifier).build()
					val content = EntityUtils.toString(httpClient.execute(new HttpGet("https://randomuser.me/api/0.8/?results=500")).getEntity)

					val urlstring = content.mkString
					val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					.set("spark.driver.allowMultipleContexts","true")

					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")


					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._


					val rdd = sc.parallelize(List(urlstring))
					val df = spark.read.json(rdd)

					println
					println
					println("======raw json api data")
					println
					println

					df.show()
					val arrayflatten = df.withColumn("results",expr("explode(results)"))
					val finalflatten = arrayflatten.select(
							"nationality",
							"results.user.cell",
							"results.user.username",
							"results.user.dob",
							"results.user.email",
							"results.user.gender",
							"results.user.location.city",
							"results.user.location.state",
							"results.user.location.street",
							"results.user.location.zip",
							"results.user.md5",
							"results.user.name.first",
							"results.user.name.last",
							"results.user.name.title",
							"results.user.password",
							"results.user.phone",
							"results.user.picture.large",
							"results.user.picture.medium",
							"results.user.picture.thumbnail",
							"results.user.registered",
							"results.user.salt",
							"results.user.sha1",
							"results.user.sha256",
							"seed",
							"version"
							)
					println("======flatten data")
	
					finalflatten.show()


					val avrodf = spark.read.format("avro")
					.load("file:///C:/data/projectsample.avro")
					println("======avro data")
	
					avrodf.show()
					avrodf.printSchema()
    
					val numdf = finalflatten.withColumn("username",regexp_replace(col("username"),  "([0-9])", ""))
		
					println("======numericals removed data")
					numdf.show()
					println("======joined data")

					val joindf = avrodf.join(numdf,Seq("username"),"left")

					joindf.show()

					println("======available data")			
					
          val availablecustomerinapi = joindf.filter(col("nationality").isNotNull)
					
					availablecustomerinapi.show()

					println("======Not available data")

					val notavailablecustomerinapi = joindf.filter(col("nationality").isNull)
					notavailablecustomerinapi.show()

					availablecustomerinapi.write.format("parquet").mode("append").save("file:///C:/data/projwrite/available")

						notavailablecustomerinapi.write.format("parquet").mode("append").save("file:///C:/data/projwrite/notavailable")


	}
}
