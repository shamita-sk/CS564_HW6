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
	AttrDesc attrDesc;
	AttrDesc projAttrDesc[projCnt];
	
	// For each projection, get description of attributes
	for(int i = 0; i < projCnt; i++)
	{
		status = attrCat->getInfo(projNames[i].relName,projNames[i].attrName,attrDesc);
		if(status != OK) return status;

		projAttrDesc[i] = attrDesc;
	}

	// If there is a WHERE clause, get description of attribute in WHERE clause. Otherwise, no filter
	AttrDesc* attrDescPointer = nullptr;
	int attrLength = 0;
	if(attr != nullptr)
	{
		attrDescPointer = new AttrDesc;
		status = attrCat->getInfo(attr->relName,attr->attrName,*attrDescPointer);
		if(status != OK) return status;
		attrLength = attrDescPointer->attrLen;
	} 

	return ScanSelect(result, projCnt, projAttrDesc, attrDescPointer, op, attrValue, attrLength);
}


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


}
