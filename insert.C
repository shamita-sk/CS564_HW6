/*
Oscar Zapata - 908 440 2404
Shamita Senthil Kumar - 908 542 2054
Jerry - 908 364 6084
*/

#include "catalog.h"
#include "query.h"
#include "heapfile.h"
#include <cstring>
#include <cstdlib>
#include <iostream> // for debugging ONLY

const Status QU_Insert(const string & relation, 
    const int attrCnt, 
    const attrInfo attrList[])
{
    Status status;
    int relAttrCount;
    AttrDesc *relAttrs;
    
    // Get relation information
    status = attrCat->getRelInfo(relation, relAttrCount, relAttrs);
    if (status != OK) {
        return status;
    }
    
    // Calculate record length
    int recLen = 0;
    for (int i = 0; i < relAttrCount; i++) {
        recLen += relAttrs[i].attrLen;
    }
    
    // Create and zero the record buffer
    char *record = new char[recLen];
    memset(record, 0, recLen);
    
    // Process each attribute in the relation schema
    for (int i = 0; i < relAttrCount; i++) {
        bool found = false;
        
        // Find the corresponding attribute in attrList
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(relAttrs[i].attrName, attrList[j].attrName) == 0) {
                found = true;
                
                // Process based on attribute type
                if (relAttrs[i].attrType == INTEGER) {
                    // Handle integer attributes
                    int value = 0;
                    
                    // Direct conversion from string
                    if (attrList[j].attrValue != NULL) {
                        value = atoi((const char*)attrList[j].attrValue);
                    }
                    
                    // Place the integer value directly in the record
                    *((int*)(record + relAttrs[i].attrOffset)) = value;
                }
                else if (relAttrs[i].attrType == FLOAT) {
                    // Handle float attributes
                    float value = 0.0;
                    
                    // Direct conversion from string
                    if (attrList[j].attrValue != NULL) {
                        value = (float)atof((const char*)attrList[j].attrValue);
                    }
                    
                    // Place the float value directly in the record
                    *((float*)(record + relAttrs[i].attrOffset)) = value;
                }
                else if (relAttrs[i].attrType == STRING) {
                    // Handle string attributes
                    if (attrList[j].attrValue != NULL) {
                        // Compute string length safely
                        const char* strVal = (const char*)attrList[j].attrValue;
                        int strLen = strlen(strVal);
                        
                        // Ensure we don't overflow the field
                        int copyLen = (strLen < relAttrs[i].attrLen) ? strLen : relAttrs[i].attrLen - 1;
                        
                        // Copy the string and ensure null termination
                        strncpy(record + relAttrs[i].attrOffset, strVal, copyLen);
                        record[relAttrs[i].attrOffset + copyLen] = '\0';
                    }
                }
                break;
            }
        }
        
        // Check if we found the attribute
        if (!found) {
            delete [] relAttrs;
            delete [] record;
            return ATTRNOTFOUND;
        }
    }
    
    // Prepare to insert
    InsertFileScan scan(relation, status);
    if (status != OK) {
        delete [] relAttrs;
        delete [] record;
        return status;
    }
    
    // Create Record
    Record rec;
    rec.data = record;
    rec.length = recLen;
    
    // Insert
    RID rid;
    status = scan.insertRecord(rec, rid);
    
    // Clean up
    delete [] relAttrs;
    delete [] record; 
    
    return status;
}
