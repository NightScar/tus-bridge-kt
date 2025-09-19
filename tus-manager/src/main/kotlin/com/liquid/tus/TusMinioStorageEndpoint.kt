package com.liquid.tus

import arrow.core.raise.Effect
import arrow.core.raise.effect
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.collect.HashMultimap
import com.liquid.tus.TusLogic.BusinessMeta
import io.minio.*
import io.minio.messages.Part
import jakarta.servlet.http.HttpServletRequest
import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.javatime.CurrentDateTime
import org.jetbrains.exposed.sql.javatime.datetime
import org.jetbrains.exposed.sql.json.jsonb
import org.jetbrains.exposed.sql.transactions.transaction
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.io.InputStream
import java.time.LocalDateTime

@RestController
@RequestMapping("/tus/minio")
class TusMinioStorageEndpoint(
    private val minioConfigProperties: MinioConfigProperties,
    private val exposedDatabaseConfig: ExposedDatabaseConfig,
) {

    private val minioAsyncClient = MinioAsyncClient.builder()
        .endpoint(minioConfigProperties.minioEndpoint)
        .credentials(
            minioConfigProperties.minioAccessKey,
            minioConfigProperties.minioSecretKey
        )
        .build()
    private val repository = TusMinioStorageRepository(minioAsyncClient, exposedDatabaseConfig.defaultDb)


    companion object {
        const val MAX_LENGTH: Long = 1024 * 1024 * 200
    }

    @RequestMapping(value = ["/upload"], method = [RequestMethod.OPTIONS])
    fun options(): ResponseEntity<*> {
        val r = TusLogic.handleOptions(MAX_LENGTH).blockCall()
        return ResponseEntity.status(r.status).also {
            r.headers.forEach { (k, v) -> it.header(k, v) }
        }.build<Unit>()
    }

    @RequestMapping(value = ["/upload/{uploadId}"], method = [RequestMethod.HEAD])
    fun head(
        @PathVariable uploadId: String,
        @RequestHeader headers: Map<String, String>,
    ): ResponseEntity<*> {
        val r = TusLogic.handleHead(
            uploadId = uploadId,
            lowCaseRequestHeaders = headers,
            storageRepository = repository,
        ).blockCall()
        return ResponseEntity.status(r.status).also {
            r.headers.forEach { (k, v) -> it.header(k, v) }
        }.body(r.body)
    }

    @PostMapping(value = ["/upload"])
    fun post(
        @RequestHeader headers: Map<String, String>,
    ): ResponseEntity<*> {
        val r = TusLogic.handleCreation(
            lowCaseRequestHeaders = headers,
            storageRepository = repository,
            maxLength = MAX_LENGTH,
            locationGen = { "/tus/minio/upload$it" },
            businessMetaChecker = { (uploadId, uploadMeta) ->
                effect {
                    val filename = uploadMeta["filename"]!!
                    val t = filename.substringAfterLast(".")
                    val pre = filename.substringBeforeLast(".")
                    // do your business
                    TusMinioStorageRepository.MinioStorageBusinessMeta(
                        bucket = minioConfigProperties.minioBucket,
                        objectName = "${pre}_${uploadId.subSequence(0, 4)}.${t}",
                    )
                }
            }
        ).blockCall()
        return ResponseEntity.status(r.status).also {
            r.headers.forEach { (k, v) -> it.header(k, v) }
        }.body(r.body)
    }

    @PatchMapping(value = ["/upload/{uploadId}"])
    fun patch(
        @PathVariable uploadId: String,
        @RequestHeader headers: Map<String, String>,
        request: HttpServletRequest,
    ): ResponseEntity<*> {
        val r = TusLogic.handlePatch(
            uploadId = uploadId,
            lowCaseRequestHeaders = headers,
            fileInputStream = request.inputStream,
            storageRepository = repository,
        ).blockCall()
        return ResponseEntity.status(r.status).also {
            r.headers.forEach { (k, v) -> it.header(k, v) }
        }.body(r.body)
    }

}


class TusMinioStorageRepository(
    private val minioClient: MinioAsyncClient,
    private val db: Database,
) : TusLogic.StorageRepository<TusMinioStorageRepository.MinioStorageBusinessMeta> {

    companion object {
        val om = jacksonObjectMapper()
    }

    init {
        transaction(db) {
            if (!MinioUploadInfoTable.exists()) {
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
            serialize = { om.writeValueAsString(it) },
            deserialize = { om.readValue<Map<String, String?>>(it) }
        )
        val businessMeta = jsonb(
            "business_meta",
            serialize = { om.writeValueAsString(it) },
            deserialize = { om.readValue<MinioStorageBusinessMeta>(it) }
        )
        val finished = bool("finished").default(false)
        val createTime = datetime("create_time").defaultExpression(CurrentDateTime)
        val updateTime = datetime("update_time").defaultExpression(CurrentDateTime)
    }


    data class MinioStorageBusinessMeta(
        val bucket: String,
        val objectName: String,
        val multiPartInfo: MultiPartInfo? = null,
    ) : BusinessMeta

    data class MultiPartInfo(
        val minioUploadId: String,
        val partSize: Long,
        val partCount: Int,
        val parts: List<MinioPart> = emptyList(),
    )
    data class MinioPart(
        val partNumber: Int,
        val etag: String,
    )

    override fun createUploadInfo(
        uploadId: String,
        totalSize: Long,
        uploadMeta: Map<String, String?>,
        businessMeta: MinioStorageBusinessMeta,
    ): Effect<TusError, TusLogic.UploadInfo> = effect {

        // check bucket
        ensure(
            minioClient.bucketExists(
                BucketExistsArgs.builder().bucket(businessMeta.bucket).build()
            ).get()
        ) {
            TusError.TusBusinessError("bucket not exist")
        }

        transaction(db) {
            val existing = MinioUploadInfoTable.selectAll().where {
                MinioUploadInfoTable.uploadId eq uploadId
            }.firstOrNull()
            ensure(existing == null) {
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
            MinioUploadInfoTable.selectAll().where {
                MinioUploadInfoTable.uploadId eq uploadId
            }.map {
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
        ensureNotNull(info) {
            TusError.TusBusinessError("create upload info failed")
        }
    }

    override fun getUploadInfo(uploadId: String): Effect<TusError, TusLogic.UploadInfo?> = effect {
        transaction(db) {
            MinioUploadInfoTable.selectAll().where {
                MinioUploadInfoTable.uploadId eq uploadId
            }.map {
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

    @Suppress("LongMethod")
    override fun writeDataAndUpdateOffset(
        uploadId: String,
        offset: Long,
        dataSize: Long,
        inputStream: InputStream
    ): Effect<TusError, TusLogic.UploadInfo> = effect {
        val uploadInfo = ensureNotNull(getUploadInfo(uploadId).bind()) {
            TusError.UploadNotFound(uploadId)
        }
        val businessMeta = uploadInfo.businessMeta
        val filename = uploadInfo.uploadMeta["filename"]!!
        val contentType = filename.substringAfterLast(".", "else").let {
            MinioContentType.contentType(it)
        }
        ensure(businessMeta is MinioStorageBusinessMeta) {
            TusError.TusBusinessError("mis match business meta type")
        }

        if (offset == 0L && dataSize == uploadInfo.totalSize) {
            // 第一次上传，文件大小与totalSize一致，直接写入
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(businessMeta.bucket)
                    .`object`(businessMeta.objectName)
                    .stream(inputStream, dataSize, -1)
                    .contentType(contentType)
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
            return@effect ensureNotNull(getUploadInfo(uploadId).bind()) {
                TusError.UploadNotFound(uploadId)
            }
        }
        val multiPartInfo: MultiPartInfo = if (offset == 0L) {
            // 第一次上传，offset为0，并且是分片的
            // 此时, 分片大小 = dataSize
            val partCount = (uploadInfo.totalSize + dataSize - 1) / dataSize

            // 创建分片上传的 minio upload id
            val createMultipartUploadResponse: CreateMultipartUploadResponse =
                minioClient.createMultipartUploadAsync(
                    businessMeta.bucket,
                    null,
                    businessMeta.objectName,
                    HashMultimap.create<String, String>().apply {
                        put("Content-Type", contentType)
                    },
                    null
                ).get()
            val minioUploadId = createMultipartUploadResponse.result().uploadId()
            MultiPartInfo(
                minioUploadId = minioUploadId,
                partSize = dataSize,
                partCount = partCount.toInt(),
            )
        } else {
            // 非第一个分片
            ensureNotNull(businessMeta.multiPartInfo) {
                TusError.TusBusinessError("minio multiPartInfo is null")
            }
        }
        val partNumber = (offset / multiPartInfo.partSize).toInt() + 1

        val uploadPartResponse: UploadPartResponse = minioClient.uploadPartAsync(
            businessMeta.bucket,
            null,
            businessMeta.objectName,
            inputStream,
            dataSize,
            multiPartInfo.minioUploadId,
            partNumber,
            null,
            null
        ).get()
        val eTag = uploadPartResponse.etag()
        val part = MinioPart(partNumber, eTag)

        val newMultiPartInfo = multiPartInfo.copy(
            parts = multiPartInfo.parts + part
        )

        if (offset + dataSize == uploadInfo.totalSize) {
            // finished

            minioClient.completeMultipartUploadAsync(
                businessMeta.bucket,
                null,
                businessMeta.objectName,
                newMultiPartInfo.minioUploadId,
                newMultiPartInfo.parts.map{Part(it.partNumber, it.etag)}.toTypedArray(),
                null,
                null
            ).get()

            transaction(db) {
                MinioUploadInfoTable.update({
                    (MinioUploadInfoTable.uploadId eq uploadId) and
                            (MinioUploadInfoTable.offset eq offset)
                }) {
                    it[MinioUploadInfoTable.offset] = offset + dataSize
                    it[MinioUploadInfoTable.finished] = true
                    it[MinioUploadInfoTable.businessMeta] = businessMeta.copy(
                        multiPartInfo = newMultiPartInfo
                    )
                    it[MinioUploadInfoTable.updateTime] = LocalDateTime.now()
                }
            }
        } else {
            transaction(db) {
                MinioUploadInfoTable.update({
                    (MinioUploadInfoTable.uploadId eq uploadId) and
                            (MinioUploadInfoTable.offset eq offset)
                }) {
                    it[MinioUploadInfoTable.offset] = offset + dataSize
                    it[MinioUploadInfoTable.businessMeta] = businessMeta.copy(
                        multiPartInfo = newMultiPartInfo
                    )
                    it[MinioUploadInfoTable.updateTime] = LocalDateTime.now()
                }
            }
        }
        getUploadInfo(uploadId).bind()!!
    }

    override fun deleteUploadInfo(uploadId: String): Effect<TusError, Unit> = effect {
        val uploadInfo = ensureNotNull(getUploadInfo(uploadId).bind()) {
            TusError.UploadNotFound(uploadId)
        }
        val businessMeta = uploadInfo.businessMeta
        ensure(businessMeta is MinioStorageBusinessMeta) {
            TusError.TusBusinessError("mis match business meta type")
        }
        if (businessMeta.multiPartInfo != null && !uploadInfo.finished) {
            minioClient.abortMultipartUploadAsync(
                businessMeta.bucket,
                null,
                businessMeta.objectName,
                businessMeta.multiPartInfo.minioUploadId,
                null,
                null
            ).get()
        }
        transaction(db) {
            MinioUploadInfoTable.deleteWhere {
                (MinioUploadInfoTable.uploadId eq uploadId)
            }
        }
    }
}
