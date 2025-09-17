package com.liquid.tus

import arrow.core.raise.Effect
import arrow.core.raise.effect
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.liquid.tus.TusLogic.BusinessMeta
import io.minio.BucketExistsArgs
import io.minio.CreateMultipartUploadResponse
import io.minio.MinioAsyncClient
import io.minio.PutObjectArgs
import io.minio.UploadPartResponse
import io.minio.messages.Part
import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.exists
import org.jetbrains.exposed.sql.insertAndGetId
import org.jetbrains.exposed.sql.javatime.CurrentDateTime
import org.jetbrains.exposed.sql.javatime.datetime
import org.jetbrains.exposed.sql.json.jsonb
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.io.File
import java.io.InputStream
import java.io.RandomAccessFile
import java.time.LocalDateTime
import kotlin.ByteArray
import kotlin.Int
import kotlin.Long
import kotlin.String
import kotlin.TODO
import kotlin.Unit
import kotlin.also
import kotlin.collections.Map
import kotlin.collections.firstOrNull
import kotlin.collections.map
import kotlin.collections.plus
import kotlin.plus
import kotlin.sequences.plus
import kotlin.text.get
import kotlin.text.plus

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
        val updateTime = datetime("update_time").defaultExpression(CurrentDateTime)
    }


    data class MinioStorageBusinessMeta(
        val bucket: String,
        val objectName: String,
        val minioUploadId: String? = null,
        val parts: List<Part> = emptyList(),
    ) : BusinessMeta

    override fun createUploadInfo(
        uploadId: String,
        totalSize: Long,
        uploadMeta: Map<String, String?>,
        businessMeta: MinioStorageBusinessMeta,
    ): Effect<TusError, TusLogic.UploadInfo> = effect{

        // check bucket
        ensure(minioClient.bucketExists(
            BucketExistsArgs.builder().bucket(businessMeta.bucket).build()
        ).get()){
            TusError.TusBusinessError("bucket not exist")
        }

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
                    updateTime = it[MinioUploadInfoTable.updateTime],
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
                    updateTime = it[MinioUploadInfoTable.updateTime],
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
        ensure(businessMeta is MinioStorageBusinessMeta){
            TusError.TusBusinessError("mis match business meta type")
        }

        if(offset == 0L && dataSize == uploadInfo.totalSize){
            // 第一次上传，文件大小与totalSize一致，直接写入
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(businessMeta.bucket)
                    .`object`(businessMeta.objectName)
                    .stream(inputStream, dataSize, -1)
                    .contentType("") // todo: content type
                    .build()
            ).get()
            transaction(db) {
                MinioUploadInfoTable.update({
                    (MinioUploadInfoTable.uploadId eq uploadId) and
                            (MinioUploadInfoTable.offset eq 0L)
                }) {
                    it[MinioUploadInfoTable.offset] = offset
                    it[MinioUploadInfoTable.finished] = true
                    it[MinioUploadInfoTable.updateTime] = LocalDateTime.now()
                }
            }
            return@effect ensureNotNull(getUploadInfo(uploadId).bind()){
                TusError.UploadNotFound(uploadId)
            }
        }
        val minioUploadId : String = if(offset == 0L){
            // 第一次上传，offset为0，并且是分片的
            // 此时, 分片大小 = dataSize

            // 创建分片上传的 minio upload id
            val createMultipartUploadResponse: CreateMultipartUploadResponse =
                minioClient.createMultipartUploadAsync(
                    businessMeta.bucket,
                    null,
                    businessMeta.objectName,
                    null,
                    null
                ).get()
            createMultipartUploadResponse.result().uploadId()
        }else{
            // 非第一个分片
            ensureNotNull(businessMeta.minioUploadId){
                TusError.TusBusinessError("minio upload id is null")
            }
        }

        val uploadPartResponse: UploadPartResponse = minioClient.uploadPartAsync(
                businessMeta.bucket,
                null,
                businessMeta.objectName,
                inputStream,
                dataSize,
                minioUploadId,
                1123,
                null,
                null
            ).get()
        val eTag = uploadPartResponse.etag()

        if(offset + dataSize == uploadInfo.totalSize){
            // finished
        }
    }

    override fun deleteUploadInfo(uploadId: String): Effect<TusError, Unit> {
        TODO("Not yet implemented")
    }
}
