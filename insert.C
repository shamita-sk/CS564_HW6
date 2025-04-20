#include "catalog.h"
#include "query.h"
#include "heapfile.h"
#include "string.h"

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
    
    // Create a record buffer to hold the tuple data
    int recLen = 0;
    for (int i = 0; i < relAttrCount; i++) {
        recLen += relAttrs[i].attrLen;
    }
    
    char *record = new char[recLen];
    memset(record, 0, recLen);  // Initialize the record buffer with zeros
    
    // For each attribute in the relation, find its value in attrList and copy it to the record
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
                
                // Copy the attribute value to the correct offset in the record
                memcpy(record + relAttrs[i].attrOffset, attrList[j].attrValue, relAttrs[i].attrLen);
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
