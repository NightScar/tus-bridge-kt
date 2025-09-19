package com.liquid.tus

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "minio")
data class MinioConfigProperties(
    var servicePreUrl: String = "",
    var minioEndpoint: String = "",
    var minioAccessKey: String = "",
    var minioSecretKey: String = "",
    var minioBucket: String = "",
)


@Configuration
@ConfigurationProperties(prefix = "datasource")
data class DatabaseConfigProperties(
    var url: String = "",
    var driverClassName: String = "",
    var username: String = "",
    var password: String = "",
)

private val log = KotlinLogging.logger {}

@Configuration
class ExposedDatabaseConfig(
    val dbProps: DatabaseConfigProperties,
) {

    val defaultDb by lazy {
        Database.connect(hikariDataSource())
    }

    init {
        log.info{"Initializing ExposedDatabaseConfig with:"}
        log.info{"dbUrl: {${dbProps.url}}"}
        log.info{"driverClassName: ${dbProps.driverClassName}" }
        log.info{"username: ${dbProps.username}" }
        TransactionManager.defaultDatabase = defaultDb
    }

    fun hikariDataSource(): HikariDataSource {
        val config = HikariConfig().apply {
            jdbcUrl = dbProps.url
            driverClassName = dbProps.driverClassName
            username = dbProps.username
            password = dbProps.password
            maximumPoolSize = MAXIMUM_POOL_SIZE
            isReadOnly = false
            transactionIsolation = "TRANSACTION_SERIALIZABLE"
        }
        val h = HikariDataSource(config)
        return h
    }

    companion object {
        const val MAXIMUM_POOL_SIZE = 10
    }
}
