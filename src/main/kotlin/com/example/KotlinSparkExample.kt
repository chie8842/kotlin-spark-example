package com.example
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import kotlin.reflect.KClass

object KotlinSparkExample {
    @JvmStatic fun main(args: Array<String>) {
        val conf = SparkConf()
                .setMaster("local")
                .setAppName("Kotlin Spark Test")

        val sc = JavaSparkContext(conf)
        val item = arrayListOf(1, 2, 3, 4, 5)

        val input = sc.parallelize(item)
        val a = input.map { it -> "num:" + it }.count()

        println(a)
    }
}

