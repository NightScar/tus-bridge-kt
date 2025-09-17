package com.liquid.tus

import org.junit.jupiter.api.Assertions.*
import kotlin.test.Test

class TusLogicMetadataParseTest {

    @Test
    fun `parseMetaData should return empty map when metaDataStr is empty`() {
        val metaDataStr = ""
        val result = TusLogic.parseMetaData(metaDataStr)
        assertTrue(result.isEmpty())
    }

    @Test
    fun `parseMetaData should return map with key value pair when metaDataStr is not empty`() {
        val metaDataStr = "filename d29ybGRfZG9taW5hdGlvbl9wbGFuLnBkZg=="
        val result = TusLogic.parseMetaData(metaDataStr)
        assertEquals(1, result.size)
        assertEquals("world_domination_plan.pdf", result["filename"])
    }

    @Test
    fun `parseMetaData should return map with key when not value`() {
        val metaDataStr = "filename d29ybGRfZG9taW5hdGlvbl9wbGFuLnBkZg==,is_flag"
        val result = TusLogic.parseMetaData(metaDataStr)
        assertEquals(2, result.size)
        assertEquals("world_domination_plan.pdf", result["filename"])
        assertEquals(true, result.containsKey("is_flag"))
    }

}