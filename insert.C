#include "catalog.h"
#include "query.h"
#include "heapfile.h"
#include "string.h"
#include <cstdlib> // For atoi and atof functions

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
    Status status;
    int relAttrCount;           // Number of attributes in the relation
    AttrDesc *relAttrs;         // Array of attribute descriptors of the relation
    
    // Get relation information
    status = attrCat->getRelInfo(relation, relAttrCount, relAttrs);
    if (status != OK) {
        return status;
    }
    
    // Check if the number of attributes provided matches the relation schema
    if (attrCnt != relAttrCount) {
        delete [] relAttrs;
        return ATTRTYPEMISMATCH;
    }
    
    // Calculate record length
    int recLen = 0;
    for (int i = 0; i < relAttrCount; i++) {
        recLen += relAttrs[i].attrLen;
    }
    
    // Create a record buffer with proper initialization
    char *record = new char[recLen];
    memset(record, 0, recLen);
    
    // For each attribute in the relation schema
    for (int i = 0; i < relAttrCount; i++) {
        bool attrFound = false;
        
        // Find the attribute in attrList
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(relAttrs[i].attrName, attrList[j].attrName) == 0) {
                attrFound = true;
                
                // Check if the attribute types match
                if (relAttrs[i].attrType != attrList[j].attrType) {
                    delete [] relAttrs;
                    delete [] record;
                    return ATTRTYPEMISMATCH;
                }
                
                // Handle different attribute types
                if (relAttrs[i].attrType == INTEGER) {
                    // For integers, convert string to int if needed
                    int value;
                    if (attrList[j].attrValue != nullptr) {
                        // If attrValue is a char*, convert it to int
                        if (attrList[j].attrLen == 0) {
                            value = atoi((char*)attrList[j].attrValue);
                        } else {
                            // If it's already an integer in binary form
                            value = *((int*)attrList[j].attrValue);
                        }
                    } else {
                        delete [] relAttrs;
                        delete [] record;
                        return ATTRNOTFOUND;
                    }
                    // Copy the integer value to the record at the proper offset
                    memcpy(record + relAttrs[i].attrOffset, &value, sizeof(int));
                }
                else if (relAttrs[i].attrType == FLOAT) {
                    // For floats, convert string to float if needed
                    float value;
                    if (attrList[j].attrValue != nullptr) {
                        // If attrValue is a char*, convert it to float
                        if (attrList[j].attrLen == 0) {
                            value = (float)atof((char*)attrList[j].attrValue);
                        } else {
                            // If it's already a float in binary form
                            value = *((float*)attrList[j].attrValue);
                        }
                    } else {
                        delete [] relAttrs;
                        delete [] record;
                        return ATTRNOTFOUND;
                    }
                    // Copy the float value to the record at the proper offset
                    memcpy(record + relAttrs[i].attrOffset, &value, sizeof(float));
                }
                else if (relAttrs[i].attrType == STRING) {
                    // For strings, just copy the data
                    if (attrList[j].attrValue != nullptr) {
                        // Make sure we don't overflow the attribute length
                        int copyLen = strlen((char*)attrList[j].attrValue);
                        if (copyLen > relAttrs[i].attrLen)
                            copyLen = relAttrs[i].attrLen;
                        
                        // Copy the string value
                        strncpy(record + relAttrs[i].attrOffset, 
                                (char*)attrList[j].attrValue, 
                                copyLen);
                    } else {
                        delete [] relAttrs;
                        delete [] record;
                        return ATTRNOTFOUND;
                    }
                }
                break;
            }
        }
        
        // If attribute not found, return error
        if (!attrFound) {
            delete [] relAttrs;
            delete [] record;
            return ATTRNOTFOUND;
        }
    }
    
    // Insert the record into the relation
    InsertFileScan scan(relation, status);
    if (status != OK) {
        delete [] relAttrs;
        delete [] record;
        return status;
    }
    
    // Create a Record object from the char buffer
    Record rec;
    rec.data = record;
    rec.length = recLen;
    
    RID rid;
    status = scan.insertRecord(rec, rid);
    
    // Clean up
    delete [] relAttrs;
    delete [] record;
    
    return status;
}
