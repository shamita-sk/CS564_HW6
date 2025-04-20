/*
Oscar Zapata - 908 440 2404
Shamita Senthil Kumar - 908 542 2054
Jerry - 908 364 6084
*/
#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{

	Status status;
	RID rid;
	AttrDesc attrDesc;
	HeapFileScan *scanner;

	// check scanner object
	scanner = new HeapFileScan(relation, status);
	if (status != OK) return status;

	// checking attribute information
	attrCat->getInfo(relation, attrName, attrDesc);
	
	int offsetVal = attrDesc.attrOffset;
	int lengthVal = attrDesc.attrLen;
	int intVal;
	float floatVal;

	// checking types of condition, do scans accordingly
	switch(type)
	{
		case INTEGER:
			intVal = atoi(attrValue);
			status = scanner->startScan(offsetVal, lengthVal, type,(char *)&intVal, op);
			break;
		case FLOAT:
			floatVal = atof(attrValue);
			status = scanner->startScan(offsetVal, lengthVal, type,(char *)&floatVal, op);
			break;
		case STRING:
			status = scanner->startScan(offsetVal, lengthVal, type, attrValue, op);
			break;
	}
	//delete scanner if the status isn't correct
	if (status != OK)
	{
		delete scanner;
		return status;
	}

	// iterating through and checking the next scanner object; delete records accordingly
	while ((status = scanner->scanNext(rid)) == OK)
	{
		if (status != OK) return status;
		status = scanner->deleteRecord();
		if (status != OK) return status;
	}

	// ending scan
	scanner->endScan();
	delete scanner;

	return OK;
}


