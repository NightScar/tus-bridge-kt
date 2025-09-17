package com.liquid.tus

import arrow.core.raise.Effect
import arrow.core.raise.effect
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.liquid.tus.TusLogic.BusinessMeta
import io.minio.MinioAsyncClient
import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.exists
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.insertAndGetId
import org.jetbrains.exposed.sql.javatime.CurrentDateTime
import org.jetbrains.exposed.sql.javatime.datetime
import org.jetbrains.exposed.sql.json.jsonb
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.io.File
import java.io.InputStream
import java.io.RandomAccessFile
import java.time.LocalDateTime

@RestController
@RequestMapping("/tus/minio")
class TusMinioStorageEndpoint(
    private val minioClient: MinioAsyncClient,
) {


}



class TusMinioStorageRepository(
    private val minioClient: MinioAsyncClient,
    private val db: Database,
) : TusLogic.StorageRepository<TusMinioStorageRepository.MinioStorageBusinessMeta> {

    companion object{
        val om = jacksonObjectMapper()
    }

    init {
        transaction(db) {
            if(!MinioUploadInfoTable.exists()){
                SchemaUtils.create(MinioUploadInfoTable)
            }
        }
    }

    // you can use cache, here we use database
    object MinioUploadInfoTable : LongIdTable("tus_minio_upload_info") {
        val uploadId = varchar("upload_id", 255).index()

        val totalSize = long("total_size")
        val offset = long("offset").default(0L)
        val uploadMeta = jsonb(
            "upload_meta",
            serialize = {om.writeValueAsString(it)},
            deserialize = {om.readValue<Map<String, String?>>(it)}
        )
        val businessMeta = jsonb(
            "business_meta",
            serialize = {om.writeValueAsString(it)},
            deserialize = {om.readValue<MinioStorageBusinessMeta>(it)}
        )
        val finished = bool("finished").default(false)
        val createTime = datetime("create_time").defaultExpression(CurrentDateTime)
    }


    data class MinioStorageBusinessMeta(
        val bucket: String,
        val objectName: String,
    ) : BusinessMeta

    override fun createUploadInfo(
        uploadId: String,
        totalSize: Long,
        uploadMeta: Map<String, String?>,
        businessMeta: MinioStorageBusinessMeta,
    ): Effect<TusError, TusLogic.UploadInfo> = effect{

        minioClient.createMultipartUploadAsync(
            bucket = businessMeta.bucket,
            objectName = businessMeta.objectName,
        )

        transaction(db) {
            val existing = MinioUploadInfoTable.selectAll().where{
                MinioUploadInfoTable.uploadId eq uploadId
            }.firstOrNull()
            ensure(existing == null){
                TusError.TusBusinessError("upload id existed")
            }
        }

        val info = transaction(db) {
            MinioUploadInfoTable.insertAndGetId {
                it[MinioUploadInfoTable.uploadId] = uploadId
                it[MinioUploadInfoTable.totalSize] = totalSize
                it[MinioUploadInfoTable.uploadMeta] = uploadMeta
                it[MinioUploadInfoTable.businessMeta] = businessMeta
            }
            MinioUploadInfoTable.selectAll().where{
                MinioUploadInfoTable.uploadId eq uploadId
            }.map{
                TusLogic.UploadInfo(
                    id = it[MinioUploadInfoTable.id].value,
                    uploadId = it[MinioUploadInfoTable.uploadId],
                    totalSize = it[MinioUploadInfoTable.totalSize],
                    uploadMeta = it[MinioUploadInfoTable.uploadMeta],
                    businessMeta = it[MinioUploadInfoTable.businessMeta],
                    offset = it[MinioUploadInfoTable.offset],
                    finished = it[MinioUploadInfoTable.finished],
                    createTime = it[MinioUploadInfoTable.createTime],
                )
            }.firstOrNull()
        }
        ensureNotNull(info){
            TusError.TusBusinessError("create upload info failed")
        }
    }

    override fun getUploadInfo(uploadId: String): Effect<TusError, TusLogic.UploadInfo?> = effect{
        transaction(db) {
            MinioUploadInfoTable.selectAll().where{
                MinioUploadInfoTable.uploadId eq uploadId
            }.map{
                TusLogic.UploadInfo(
                    id = it[MinioUploadInfoTable.id].value,
                    uploadId = it[MinioUploadInfoTable.uploadId],
                    totalSize = it[MinioUploadInfoTable.totalSize],
                    uploadMeta = it[MinioUploadInfoTable.uploadMeta],
                    businessMeta = it[MinioUploadInfoTable.businessMeta],
                    offset = it[MinioUploadInfoTable.offset],
                    finished = it[MinioUploadInfoTable.finished],
                    createTime = it[MinioUploadInfoTable.createTime],
                )
            }.firstOrNull()
        }
    }

    override fun writeDataAndUpdateOffset(
        uploadId: String,
        offset: Long,
        dataSize: Long,
        inputStream: InputStream
    ): Effect<TusError, TusLogic.UploadInfo> = effect{
        val uploadInfo = ensureNotNull(getUploadInfo(uploadId).bind()){
            TusError.UploadNotFound(uploadId)
        }
        val businessMeta = uploadInfo.businessMeta
        ensure(businessMeta is LocalStorageBusinessMeta){
            TusError.TusBusinessError("mis match business meta type")
        }

        val filePathToWrite = uploadInfo.businessMeta.dirPath + "/" + uploadInfo.businessMeta.tmpName
        // 使用RandomAccessFile在指定offset位置写入数据
        RandomAccessFile(filePathToWrite, "rw").use { file ->
            // 定位到指定的offset位置
            file.seek(offset)

            // 从inputStream读取数据并写入文件
            val buffer = ByteArray(8192)
            var bytesRead: Int
            inputStream.use { input ->
                while (input.read(buffer).also { bytesRead = it } != -1) {
                    file.write(buffer, 0, bytesRead)
                }
            }
        }
        if(offset + dataSize == uploadInfo.totalSize){
            // finished
            val tmpFile = File(filePathToWrite)
            val finalFile = File(businessMeta.dirPath + "/" + businessMeta.writeFileName)
            tmpFile.renameTo(finalFile)
        }
    }

    override fun deleteUploadInfo(uploadId: String): Effect<TusError, Unit> {
        TODO("Not yet implemented")
    }
}
