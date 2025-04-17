/*
Oscar Zapata - 908 440 2404
Shamita Senthil Kumar - 908 542 2054
Jerry - 908 364 6084
*/

#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

	Status status;
	AttrDesc projAttrDesc[projCnt];
	int reclen = 0;
	
	// For each projection, get description of attributes
	for(int i = 0; i < projCnt; i++)
	{
		status = attrCat->getInfo(projNames[i].relName,projNames[i].attrName,projAttrDesc[i]);
		if(status != OK) return status;

		reclen += projAttrDesc[i].attrLen;
	}

	// If there is a WHERE clause, get description of attribute in WHERE clause. Otherwise, no filter
	AttrDesc attrDescVal;
	AttrDesc* attrDescPointer = NULL;
	if(attr != NULL)
	{
		status = attrCat->getInfo(attr->relName,attr->attrName,attrDescVal);
		if(status != OK) return status;
		attrDescPointer = &attrDescVal;
	} 

	return ScanSelect(result, projCnt, projAttrDesc, attrDescPointer, op, attrValue, reclen);
}

/*
* Applies filter on relation (if necessary) and projects filtered tuples on specified attributes.
*
* Returns:
* 	OK on success
* 	an error code otherwise
*/
const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;

	HeapFileScan readFile(projNames[0].relName,status);
	if(status != OK) return status;

	InsertFileScan outputFile(result,status);
	if(status != OK) return status;

	void *scanFilter = NULL;
	Datatype type;
	int intVal;
	float floatVal;

	// Start scan on readFile
	// if there is a WHERE filter
	if(filter != NULL && attrDesc != NULL)
	{
		switch(attrDesc->attrType) 
		{
			case STRING:
			{
				type = STRING;
				scanFilter = (void *) filter;
				break;
			}
			case INTEGER:
			{
				type = INTEGER;
				intVal = atoi(filter);
				scanFilter = (void *) &intVal;
				break;
			}
			case FLOAT:
			{
				type = FLOAT;
				floatVal = atof(filter);
				scanFilter = (void *) &floatVal;
				break;
			}
		}

		status = readFile.startScan(attrDesc->attrOffset,attrDesc->attrLen,type,(char *)scanFilter,op);
	} else // If there is no WHERE filter
	{
		status = readFile.startScan(0,0,STRING,NULL,EQ);
	}

	// Check status on startScan of readFile
	if (status != OK) return status;

	// Read variables
	RID rid;
	Record readRec;

	// Output variables
	RID outputRid;
	Record outputRec;
	char *outputData = new char[reclen];

	// Set pointer to data of record
	outputRec.data = (void*) outputData;
	outputRec.length = reclen;

	int offsetAcc;

	// Iterate through each record that passed the filter
	while(readFile.scanNext(rid) == OK)
	{
		status = readFile.getRecord(readRec);
		if(status != OK) 
		{
			delete[] outputData;
			return status;
		}

		offsetAcc = 0;
		// Only put in attributes in the list of projections
		for(int i = 0; i < projCnt; i++)
		{
			memcpy(outputData + offsetAcc,(char *) readRec.data + projNames[i].attrOffset,projNames[i].attrLen);

			// Update offset to account for added data
			offsetAcc += projNames[i].attrLen;
		}

		status = outputFile.insertRecord(outputRec,outputRid);
		if(status != OK) 
		{
			delete[] outputData;
			return status;
		}
	}

	delete[] outputData;

	status = readFile.endScan();
	if(status != OK) return status;

	return OK;

}
