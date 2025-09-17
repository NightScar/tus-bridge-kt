package com.liquid.tus

import arrow.core.raise.Effect
import arrow.core.raise.effect
import arrow.core.raise.ensure
import arrow.core.raise.ensureNotNull
import jakarta.servlet.http.HttpServletRequest
import okio.Buffer
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.io.File
import java.io.InputStream
import java.io.RandomAccessFile
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap

@RestController
@RequestMapping("/tus/local")
class TusLocalStorageEndpoint {

    companion object{
        const val MAX_LENGTH : Long = 1024 * 1024 * 100
    }

    @RequestMapping(value = ["/upload"], method = [RequestMethod.OPTIONS])
    fun options(): ResponseEntity<*> {
        val r = TusLogic.handleOptions(MAX_LENGTH).blockCall()
        return ResponseEntity.status(r.status).also {
            r.headers.forEach { (k,v) -> it.header(k, v) }
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
            storageRepository = TusLocalStorageRepository,
        ).blockCall()
        return ResponseEntity.status(r.status).also {
            r.headers.forEach { (k,v) -> it.header(k, v) }
        }.body(r.body)
    }

    @PostMapping(value = ["/upload"])
    fun post(
        @RequestHeader headers: Map<String, String>,
    ): ResponseEntity<*> {
        val r = TusLogic.handleCreation(
            lowCaseRequestHeaders = headers,
            storageRepository = TusLocalStorageRepository,
            maxLength = MAX_LENGTH,
            locationGen = { "/tus/local/upload$it" },
            businessMetaChecker = { (uploadId, uploadMeta) ->
                effect {
                    val filename = uploadMeta["filename"]!!
                    val t = filename.substringAfterLast(".")
                    val pre = filename.substringBeforeLast(".")
                    // do your business
                    TusLocalStorageRepository.LocalStorageBusinessMeta(
                        dirPath = "F:\\temp",
                        writeFileName = "${pre}_${uploadId.subSequence(0,4)}.${t}",
                        tmpName = "${filename}.tmp",
                    )
                }
            }
        ).blockCall()
        return ResponseEntity.status(r.status).also {
            r.headers.forEach { (k,v) -> it.header(k, v) }
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
            storageRepository = TusLocalStorageRepository,
        ).blockCall()
        return ResponseEntity.status(r.status).also {
            r.headers.forEach { (k,v) -> it.header(k, v) }
        }.body(r.body)
    }
}


object TusLocalStorageRepository : TusLogic.StorageRepository<TusLocalStorageRepository.LocalStorageBusinessMeta> {
    // memory map for demo
    private val infoMap : MutableMap<String, TusLogic.UploadInfo> = ConcurrentHashMap()

    private const val BUFFER_SIZE = 8192

    data class LocalStorageBusinessMeta(
        val dirPath: String,
        val writeFileName: String,
        val tmpName: String, // when not finished
    ) : TusLogic.BusinessMeta

    override fun createUploadInfo(
        uploadId: String,
        totalSize: Long,
        uploadMeta: Map<String, String?>,
        businessMeta: LocalStorageBusinessMeta
    ): Effect<TusError, TusLogic.UploadInfo> = effect{
        val info = TusLogic.UploadInfo(
            id = 123, // not important in demo
            uploadId = uploadId,
            totalSize = totalSize,
            uploadMeta = uploadMeta,
            businessMeta = businessMeta,
            offset = 0,
            finished = false,
            createTime = LocalDateTime.now(),
            updateTime = LocalDateTime.now(),
        )
        infoMap[uploadId] = info
        info
    }

    override fun getUploadInfo(uploadId: String): Effect<TusError, TusLogic.UploadInfo?> = effect{
        infoMap[uploadId]
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
            val buffer = ByteArray(BUFFER_SIZE)
            var bytesRead: Int
            inputStream.use { input ->
                while (input.read(buffer).also { bytesRead = it } != -1) {
                    file.write(buffer, 0, bytesRead)
                }
            }
        }
        val n = if(offset + dataSize == uploadInfo.totalSize){
            // finished
            val tmpFile = File(filePathToWrite)
            val finalFile = File(businessMeta.dirPath + "/" + businessMeta.writeFileName)
            tmpFile.renameTo(finalFile)
            uploadInfo.copy(offset=offset, finished = true, updateTime = LocalDateTime.now())
        }else{
            uploadInfo.copy(offset=offset, updateTime = LocalDateTime.now())
        }
        infoMap[uploadId] = n
        n
    }

    override fun deleteUploadInfo(uploadId: String): Effect<TusError, Unit> {
        TODO("Not yet implemented")
    }
}
