package com.liquid.tus

import arrow.core.raise.Effect
import arrow.core.raise.effect
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import arrow.core.raise.fold
import kotlinx.coroutines.runBlocking
import java.io.InputStream
import java.time.LocalDateTime
import java.util.UUID
import kotlin.io.encoding.Base64

object TusLogic {

    object Header {
        const val TUS_RESUMABLE = "Tus-Resumable"
        const val TUS_RESUMABLE_VALUE = "1.0.0"

        const val CONTENT_LENGTH = "Content-Length"
        const val UPLOAD_LENGTH = "Upload-Length"
        const val UPLOAD_METADATA = "Upload-Metadata"
        const val LOCATION = "Location"
        const val UPLOAD_OFFSET = "Upload-Offset"

        const val TUS_VERSION = "Tus-Version"
        const val TUS_VERSION_VALUE = "1.0.0"

        const val TUS_MAX_SIZE = "Tus-Max-Size"

        const val TUS_EXTENSION = "Tus-Extension"
        const val TUS_EXTENSION_VALUE = "creation,expiration"
    }

    interface BusinessMeta

    data class UploadInfo(
        val id: Long,
        val uploadId: String,
        val totalSize: Long,
        val offset: Long,
        val uploadMeta: Map<String, String?>,
        val businessMeta: BusinessMeta,
        val finished: Boolean,
        val createTime: LocalDateTime,
    )

    interface StorageRepository<T : BusinessMeta> {
        fun createUploadInfo(
            uploadId: String,
            totalSize: Long,
            uploadMeta: Map<String, String?>,
            businessMeta: T,
        ): Effect<TusError, UploadInfo>
        fun getUploadInfo(uploadId: String): Effect<TusError, UploadInfo?>
        fun writeDataAndUpdateOffset(
            uploadId: String,
            offset: Long,
            dataSize: Long,
            inputStream: InputStream
        ): Effect<TusError, UploadInfo>
        fun deleteUploadInfo(uploadId: String): Effect<TusError, Unit>
    }

    fun handleOptions(
        maxLength: Long,
    ) : Effect<TusError, TusResponse> = effect {
        TusResponse(
            status = 204,
            headers = mapOf(
                Header.TUS_RESUMABLE to Header.TUS_RESUMABLE_VALUE,
                Header.TUS_VERSION to Header.TUS_VERSION_VALUE,
                Header.TUS_MAX_SIZE to maxLength.toString(),
                Header.TUS_EXTENSION to Header.TUS_EXTENSION_VALUE,
            ),
            body = null,
        )
    }

    fun <T : BusinessMeta>handleHead(
        uploadId: String,
        lowCaseRequestHeaders : Map<String, String>,
        storageRepository: StorageRepository<T>,
    ) : Effect<TusError, TusResponse> = effect {
        // tus resumable check
        val tusResumable = ensureNotNull(lowCaseRequestHeaders[Header.TUS_RESUMABLE.lowercase()]) {
            TusError.MissingRequestHeader(Header.TUS_RESUMABLE)
        }
        ensure(tusResumable == Header.TUS_RESUMABLE_VALUE) {
            TusError.InvalidRequestHeader(Header.TUS_RESUMABLE, tusResumable)
        }
        // get upload info
        val uploadInfo = ensureNotNull(storageRepository.getUploadInfo(uploadId).bind()) {
            TusError.UploadNotFound(uploadId)
        }
        TusResponse(
            status = 204,
            headers = mapOf(
                Header.TUS_RESUMABLE to Header.TUS_RESUMABLE_VALUE,
                Header.TUS_VERSION to Header.TUS_VERSION_VALUE,
                Header.UPLOAD_OFFSET to uploadInfo.offset.toString(),
                Header.UPLOAD_LENGTH to uploadInfo.totalSize.toString(),
                "Cache-Control" to "no-store",
            ),
            body = null,
        )
    }

    /**
     * Handle creation
     *
     * @param T
     * @param lowCaseRequestHeaders headers from client
     * @param maxLength
     * @param storageRepository
     * @param locationGen should add prefix ("/upload") -> "/{business prefix}/$uploadId"
     * @param businessMetaChecker
     * @receiver
     * @receiver
     * @return
     */
    fun <T : BusinessMeta> handleCreation(
        lowCaseRequestHeaders : Map<String, String>,
        maxLength: Long,
        storageRepository: StorageRepository<T>,
        locationGen: (String) -> String,
        businessMetaChecker: (Pair<String,Map<String, String?>>) -> Effect<TusError, T>,
    ) : Effect<TusError, TusResponse> = effect {
        // tus resumable check
        val tusResumable = ensureNotNull(lowCaseRequestHeaders[Header.TUS_RESUMABLE.lowercase()]) {
            TusError.MissingRequestHeader(Header.TUS_RESUMABLE)
        }
        ensure(tusResumable == Header.TUS_RESUMABLE_VALUE) {
            TusError.InvalidRequestHeader(Header.TUS_RESUMABLE, tusResumable)
        }
        // content length must be 0
        val contentLengthStr = ensureNotNull(lowCaseRequestHeaders[Header.CONTENT_LENGTH.lowercase()]) {
            TusError.MissingRequestHeader(Header.CONTENT_LENGTH)
        }
        ensure(contentLengthStr.toInt() == 0) {
            TusError.InvalidRequestHeader(Header.CONTENT_LENGTH, contentLengthStr)
        }
        // upload length check
        val uploadLengthStr = ensureNotNull(lowCaseRequestHeaders[Header.UPLOAD_LENGTH.lowercase()]) {
            TusError.MissingRequestHeader(Header.UPLOAD_LENGTH)
        }
        ensure(uploadLengthStr.toInt() > 0) {
            TusError.InvalidRequestHeader(Header.UPLOAD_LENGTH, uploadLengthStr)
        }
        val fileLength : Long = uploadLengthStr.toLong()
        ensure(fileLength <= maxLength) {
            TusError.FileSizeExceeded(maxLength, fileLength)
        }
        // upload metadata check
        val uploadMetadataStr = ensureNotNull(lowCaseRequestHeaders[Header.UPLOAD_METADATA.lowercase()]){
            TusError.MissingRequestHeader(Header.UPLOAD_METADATA)
        }
        ensure(uploadMetadataStr.isNotBlank()) {
            TusError.InvalidRequestHeader(Header.UPLOAD_METADATA, uploadMetadataStr)
        }
        val metaData = parseMetaData(uploadMetadataStr)
        // check filename
        val filename = metaData["filename"]
        ensure(filename != null) {
            TusError.MissingUploadMetadata("filename")
        }

        // create upload info
        val uploadId = UUID.randomUUID().toString()

        // check business
        val businessMeta = businessMetaChecker(Pair(uploadId, metaData)).bind()

        storageRepository.createUploadInfo(
            uploadId = uploadId,
            totalSize = fileLength,
            uploadMeta = metaData,
            businessMeta = businessMeta,
        ).bind()
        val location = locationGen("/$uploadId")
        TusResponse(
            status = 201,
            headers = mapOf(
                Header.LOCATION to location,
                Header.TUS_RESUMABLE to Header.TUS_RESUMABLE_VALUE,
            ),
            body = null,
        )
    }

    fun <T: BusinessMeta> handlePatch(
        uploadId: String,
        lowCaseRequestHeaders : Map<String, String>,
        storageRepository: StorageRepository<T>,
        fileInputStream: InputStream,
    ) : Effect<TusError, TusResponse> = effect {
        // tus resumable check
        val tusResumable = ensureNotNull(lowCaseRequestHeaders[Header.TUS_RESUMABLE.lowercase()]) {
            TusError.MissingRequestHeader(Header.TUS_RESUMABLE)
        }
        ensure(tusResumable == Header.TUS_RESUMABLE_VALUE) {
            TusError.InvalidRequestHeader(Header.TUS_RESUMABLE, tusResumable)
        }
        // get upload info
        val uploadInfo = ensureNotNull(storageRepository.getUploadInfo(uploadId).bind()) {
            TusError.UploadNotFound(uploadId)
        }
        // content type check
        val contentType = ensureNotNull(lowCaseRequestHeaders["content-type"]) {
            TusError.MissingRequestHeader("Content-Type")
        }
        ensure(contentType == "application/offset+octet-stream") {
            TusError.InvalidRequestHeader("Content-Type", contentType)
        }
        // upload offset check
        val uploadOffsetStr = ensureNotNull(lowCaseRequestHeaders[Header.UPLOAD_OFFSET.lowercase()]) {
            TusError.MissingRequestHeader(Header.UPLOAD_OFFSET)
        }
        val uploadOffset = uploadOffsetStr.toLong()
        ensure(uploadOffset == uploadInfo.offset) {
            TusError.InvalidRequestHeader(Header.UPLOAD_OFFSET, uploadOffsetStr)
        }
        // content length check
        val contentLengthStr = ensureNotNull(lowCaseRequestHeaders[Header.CONTENT_LENGTH.lowercase()]) {
            TusError.MissingRequestHeader(Header.CONTENT_LENGTH)
        }
        val contentLength = contentLengthStr.toLong()
        ensure(contentLength > 0 && contentLength+uploadOffset<=uploadInfo.totalSize) {
            TusError.InvalidRequestHeader(Header.CONTENT_LENGTH, contentLengthStr)
        }
        // write data
        val newInfo = storageRepository.writeDataAndUpdateOffset(
            uploadId = uploadId,
            offset = uploadOffset,
            inputStream = fileInputStream,
            dataSize = contentLength,
        ).bind()

        TusResponse(
            status = 204,
            headers = mapOf(
                Header.TUS_RESUMABLE to Header.TUS_RESUMABLE_VALUE,
                Header.UPLOAD_OFFSET to newInfo.offset.toString(),
            ),
            body = Unit,
        )
    }

    fun parseMetaData(metaDataStr: String) : Map<String, String?> {
        if (metaDataStr.isBlank()) {
            return emptyMap()
        }
        return metaDataStr.split(",").associate {
            val l = it.split(" ")
            if(l.size == 2){
                l[0] to l[1]
            }else {
                l[0] to null
            }
        }.mapValues {
            if(it.value != null) {
                val decodeBytes = Base64.decode(it.value!!)
                String(decodeBytes, Charsets.UTF_8)
            }else{
                null
            }
        }
    }

}

fun Effect<TusError, TusResponse>.blockCall() : TusResponse = runBlocking{
    fold(
        { this@blockCall.bind() },
        { TusError.TusBusinessError(it.message ?: "Tus business error").toResponse() },
        { it.toResponse() },
        { it },
    )
}

sealed class TusError {

    abstract fun toResponse() : TusResponse

    data class MissingRequestHeader(
        val headerName : String,
    ) : TusError() {
        override fun toResponse() : TusResponse {
            return TusResponse(
                status = 400,
                headers = emptyMap(),
                body = "Missing request header: $headerName",
            )
        }
    }

    data class InvalidRequestHeader(
        val headerName : String,
        val headerValue : String,
    ) : TusError() {
        override fun toResponse() : TusResponse {
            return TusResponse(
                status = 400,
                headers = emptyMap(),
                body = "Invalid request header: $headerName=$headerValue",
            )
        }
    }

    data class FileSizeExceeded(
        val maxLength: Long,
        val fileLength: Long,
    ) : TusError() {
        override fun toResponse() : TusResponse {
            return TusResponse(
                status = 413,
                headers = emptyMap(),
                body = "File size exceeded: max=$maxLength, file=$fileLength",
            )
        }
    }

    data class MissingUploadMetadata(
        val metaDataName : String,
    ) : TusError() {
        override fun toResponse() : TusResponse {
            return TusResponse(
                status = 400,
                headers = emptyMap(),
                body = "Missing upload metadata: $metaDataName",
            )
        }
    }

    data class TusBusinessError(
        val message : String,
    ) : TusError() {
        override fun toResponse() : TusResponse {
            return TusResponse(
                status = 400,
                headers = emptyMap(),
                body = message,
            )
        }
    }

    data class UploadNotFound(
        val uploadId : String,
    ) : TusError() {
        override fun toResponse() : TusResponse {
            return TusResponse(
                status = 404,
                headers = emptyMap(),
                body = "Upload not found: $uploadId",
            )
        }
    }
}


data class TusResponse(
    val status : Int,
    val headers : Map<String, String>,
    val body: Any?,
)
