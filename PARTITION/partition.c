// Author: Rachit Magon, Rishi Raj Sahu
// M.E. - Software Systems. Bits Pilani.
// This code takes input a SELECT query from stdin
// Distributes the work to multiple processors and
// returns the output
// Assumptions:
// 1. Flat File tables have comma separated values with first row as header
// 2. Column names in the query are given along with table names ie SELECT TABLE.COLUMN FROM TABLE WHERE TABLE.COLUMN=5
// 3. There are no spaces in between a single condition i.e. TABLE.COLUMN=5 is correct but TABLE.COLUMN = 5 is wrong 
// 4. Database files are present in the folder given by DBPATH+DBNAME
// 6. METADATA file for the database is present at EVERY NODE. It should have a key,value pair <*,-1>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#define DBPATH "PAR_DB_CLUST"
#define MAX_CONDITIONS 10
#define MAX_QUERY_COL 12
#define MAX_QUERY_TABLE 100
#define MAX_FILEPATH_LEN 10000
#define MAX_BUFF_LEN 10000
#define MAX_COL_NAME 100
#define MAX_DB_STRING 100
#define MAX_TABLE_ROWS 10000000
#define MAX_JOIN_ROWS 10000
#define SELECTED_ROWS_NO_TAG 2
#define SELECT_SEND_TAG 3
#define SELECTED_JOIN_NO_TAG 4
#define JOIN_ROWS_TAG 5
#define MAX_TABLE_COLS 12 //NOTE:If this is not set right then high probability of SEGMENTATION FAULT
#define MAX_TABLE_WORD 50 //NOTE:If this is not set right then high probability of SEGMENTATION FAULT
int VERBOSE = 0;
char size[50];
typedef struct Condition
{
	char* txt;
	int colIndex;
	char oP[5];
	char RHS[50];
}condition;

struct Query
{
	char tables[10][50];
	int tablesLen;
	char** columns;
	int columnsLen;
	condition conditions[MAX_CONDITIONS];
	int columnsIndex[MAX_QUERY_COL];
	int conditionsLen;
};

typedef union {
 char data[MAX_TABLE_COLS][MAX_TABLE_WORD]; 
 } row;
 
struct DataTable
{
	int noOfSelectedRows;
	int noOfSelectedCols;
	row* selectedRows;
};

struct JoinMetaData
{
	int start[8];
	int end[8];
};

void printDataTable(struct DataTable dbTable)
{
	int i,j;
	printf("printDataTable() : Enter\n");
	for(i=0;i<dbTable.noOfSelectedRows;i++)
	{
		for(j=0;j<dbTable.noOfSelectedCols;j++)
		{
			printf("%s ",dbTable.selectedRows[i].data[j]);
		}
		printf("\n");
	}
	printf("Selected Rows=%d, Selected Columns=%d\n",dbTable.noOfSelectedRows,dbTable.noOfSelectedCols);
}

void printDataTableToFile(struct DataTable dbTable)
{
	int i,j;
	FILE *fp;
	fp = fopen("OUTPUT", "w");
	printf("Printing to File, Please wait...\n");
	for(i=0;i<dbTable.noOfSelectedRows;i++)
	{
		for(j=0;j<dbTable.noOfSelectedCols;j++)
		{
			fprintf(fp,"%s ",dbTable.selectedRows[i].data[j]);
		}
		fprintf(fp,"\n");
	}
	fprintf(fp,"Selected Rows=%d, Selected Columns=%d\n",dbTable.noOfSelectedRows,dbTable.noOfSelectedCols);
	printf("Please refer to file OUTPUT\nSelected Rows=%d, Selected Columns=%d\n",dbTable.noOfSelectedRows,dbTable.noOfSelectedCols);
	fclose(fp);
}

//returns 1 if colToCheck is available in Query
//else returns 0
int checkIfColumnInQuery(struct Query query,int colToCheck)
{
	int i;
	for(i=0;i<query.columnsLen;i++)
	{
		if(colToCheck == query.columnsIndex[i])
		{
			return 1;
		}
	}
	return 0;
}

//Reads the METADATA file to get the ColumnIndex
char* getColumnIndex(char* columnName,char* dbName)
{
	char buff[MAX_FILEPATH_LEN];
	char filePath[MAX_FILEPATH_LEN];
	int colIndex = -1;
	FILE *fp;
	strcpy(filePath,dbName);
	strcat(filePath,"/METADATA-");
	strcat(filePath,size);
	fp = fopen(filePath, "r");
	if(fp==NULL)
	{
		printf("\ngetColumnIndex() ERROR: Could not Read file: %s\n",filePath);
	}
	else
	{
		while(fgets(buff,sizeof(buff),fp))
		{
			char* pch;
			pch = strtok (buff,",");
			if((pch!=NULL)&&strcmp(pch,columnName)==0)
			{
				char* temp  = strtok(NULL,",");
				if(VERBOSE){printf("getColumnIndex(): FOUND %s,%s\n",columnName,temp);}
				return temp;
			}
		}
	}
	printf("getColumnIndex(): Error-- should not have come here, columnName:%s\n",columnName);
	fclose(fp);
}


//Reads the Join Meta Data from METADATA file in the DataBase Folder
struct JoinMetaData getJoinMetaData(char* dbName)
{
	struct JoinMetaData joinMD;
	char buff[MAX_FILEPATH_LEN];
	char filePath[MAX_FILEPATH_LEN];
	int colIndex = -1;
	FILE *fp;
	strcpy(filePath,dbName);
	strcat(filePath,"/METADATA-");
	strcat(filePath,size);
	fp = fopen(filePath, "r");
	if(fp==NULL)
	{
		if(VERBOSE){printf("getJoinMetaData() ERROR: Could not Read file: %s\n",filePath);}
	}
	else
	{
		while(fgets(buff,sizeof(buff),fp))
		{
			char* pch;
			pch = strtok (buff,",");
			if((pch!=NULL)&&strcmp(pch,"joinmetadata")==0)
			{
				char* node  = strtok(NULL,",");
				int nodeINT = atoi(node);
				char* start  = strtok(NULL,",");
				int startINT = atoi(start);
				char* end  = strtok(NULL,",");
				int endINT = atoi(end);
				joinMD.start[nodeINT] = startINT;
				joinMD.end[nodeINT] = endINT;
				if(VERBOSE){printf("getJoinMetaData(): FOUND %d,%d,%d\n",nodeINT,joinMD.start[nodeINT],joinMD.end[nodeINT]);}
				
			}
		}
	}
	fclose(fp);
	return joinMD;
}

//This function will read the secondary table and divide it according to the metadata file!
//WARNING: query should have tables[1] otherwise it will crash
struct DataTable* readPartnerTablesForJoin(struct Query query,char* dbName,int world_size,int maxRows)
{
	
	/* Prepare the huge data structure to hold divided tables */
	int i;
	char* secColIndex;
	secColIndex = getColumnIndex(query.conditions[0].RHS,dbName);
	
	int partitionColIndex = atoi(secColIndex);
	if(VERBOSE){printf("readPartnerTablesForJoin(): Enter Found secColIndex:%s\n",secColIndex);}
	struct DataTable* partnerSecondaryTables;
	partnerSecondaryTables = (struct DataTable*) malloc(world_size*sizeof(struct DataTable));
	struct JoinMetaData joinMD = getJoinMetaData(dbName);
	
	for(i=0;i<world_size;i++)
	{
		partnerSecondaryTables[i].noOfSelectedRows = 0;
		partnerSecondaryTables[i].noOfSelectedCols = 0;
		partnerSecondaryTables[i].selectedRows = (row *)malloc(maxRows*sizeof(row));		
	}
	
	/* Do the seperation*/
	char buff[MAX_FILEPATH_LEN];
	char filePath[MAX_FILEPATH_LEN];
	int colIndex = -1;
	strcpy(filePath,dbName);
	strcat(filePath,"/");
	strcat(filePath,query.tables[1]);
	if(VERBOSE){printf("readPartnerTablesForJoin() file_path=%s\n",filePath);}
	FILE *fp;
	fp = fopen(filePath, "r");
	if(fp==NULL)
   	{
   		if(VERBOSE){printf("readPartnerTablesForJoin() returning, ERROR:Could not read file %s",filePath);}
   		return;
   	}	
   	else /* File opened, now start */
   	{
   		int currentColIndex;
   		char* pch;
   		while(fscanf(fp, "%s", buff)!=EOF)
   		{
   			
   			int copyTo = -1;
   			currentColIndex = 0;
   			row newRow;
   			pch = strtok (buff,",");
   			
   			while(pch != NULL)
			{
			//printf("%s ",pch);
				if(currentColIndex==partitionColIndex)
				{
					int pchINT = atoi(pch);
					int prcsr;
					for(prcsr=0;prcsr<world_size;prcsr++)
					{
						if((pchINT>=joinMD.start[prcsr])&&(pchINT<=joinMD.end[prcsr]))
						{
							copyTo = prcsr;
							break;
						}
					}
				}
				strcpy(newRow.data[currentColIndex++],pch);
				pch = strtok (NULL,",");
			}
			//printf("%s,%s goes to %d\n",newRow.data[1],newRow.data[3],copyTo);
				
			int cnt;
			if(copyTo!=-1)
			{
				for(cnt=0;cnt<currentColIndex;cnt++)
				{
					strcpy(partnerSecondaryTables[copyTo].selectedRows[partnerSecondaryTables[copyTo].noOfSelectedRows].data[cnt],newRow.data[cnt]);
					//printf("%s,%s",partnerSecondaryTables[copyTo].selectedRows[partnerSecondaryTables[copyTo].noOfSelectedRows].data[cnt],newRow.data[cnt]);
				}
				//printf("%d: %s %s %s\n",copyTo,partnerSecondaryTables[copyTo].selectedRows[partnerSecondaryTables[copyTo].noOfSelectedRows].data[0],partnerSecondaryTables[copyTo].selectedRows[partnerSecondaryTables[copyTo].noOfSelectedRows].data[1],partnerSecondaryTables[copyTo].selectedRows[partnerSecondaryTables[copyTo].noOfSelectedRows].data[2]);
				partnerSecondaryTables[copyTo].noOfSelectedRows++;
			}
   		}
   		
   		for(i=0;i<world_size;i++)
		{
			partnerSecondaryTables[i].noOfSelectedCols = currentColIndex;		
		}
   		
   	}
   	if(VERBOSE){printf("readPartnerTablesForJoin(): Exit\n");}
	return partnerSecondaryTables;
}


/*This function will read the given Query from the database dbname
Presently supports only a single WHERE A=B condition*/
struct DataTable selectFromPartitionedDatabase(struct Query query,char* dbname,int maxRows)
{
	struct DataTable dbTable;
	dbTable.noOfSelectedRows = 0;
	dbTable.noOfSelectedCols = 0;
	dbTable.selectedRows = (row *)malloc(maxRows*sizeof(row));
	char buff[MAX_BUFF_LEN];
	char filePath[MAX_FILEPATH_LEN];
	strcpy(filePath,dbname);
	strcat(filePath,"/");
	strcat(filePath,query.tables[0]);
	if(VERBOSE){printf("selectFromPartitionedDatabase() file_path=%s\n",filePath);}
	FILE *fp;
	fp = fopen(filePath, "r");
   	if(fp==NULL)
   	{
   		printf("selectFromPartitionedDatabase() returning, ERROR:Could not read file: %s",filePath);
   		return;
   	}	
   	else
   	{
   		char* pch;	
   		int colIndex=0;	
   		int colSelectedIndex = 0;
   		while(fscanf(fp, "%s", buff)!=EOF)
   		{
   			colIndex = 0;
   			colSelectedIndex = 0;
   			int copy=0;
   			//printf("\n%s\t", buff );
   			pch = strtok (buff,",");
   			while(pch != NULL)
			{
				if((query.columnsIndex[0] == -1)||(checkIfColumnInQuery(query,colIndex)==1))
				{
					strcpy(dbTable.selectedRows[dbTable.noOfSelectedRows].data[colSelectedIndex],pch);
					colSelectedIndex++;
				}
				//printf("%s|",dbTable.selectedRows[dbTable.noOfSelectedRows].data[colIndex]);
				if(colIndex==query.conditions[0].colIndex)
				{
					//printf("COLINDEX SAME: %d (%s,%s)",colIndex,query.conditions[0].oP,"=");
					if(strcmp(query.conditions[0].oP,"=")==0)
					{
						//printf("= OP SAME (%s,%s)",query.conditions[0].RHS,pch);
						if(strcmp(query.conditions[0].RHS,pch)==0)
						{
							//printf("--GOOD");
							copy=1;
						}
					}
					else if(strcmp(query.conditions[0].oP,">=")==0) /* > op only valid for int, else runtime error */
					{
						int rhsINT = atoi(query.conditions[0].RHS);
						int lhsINT = atoi(pch);
						if(lhsINT>=rhsINT)
						{
							copy=1;
						}
					}
					else if(strcmp(query.conditions[0].oP,"<=")==0) /* > op only valid for int, else runtime error */
					{
						int rhsINT = atoi(query.conditions[0].RHS);
						int lhsINT = atoi(pch);
						if(lhsINT<=rhsINT)
						{
							copy=1;
						}
					}
					else if(strcmp(query.conditions[0].oP,"!=")==0)
					{
						if(strcmp(query.conditions[0].RHS,pch)!=0)
						{
							copy=1;
						}
					}
					else if(strcmp(query.conditions[0].oP,">")==0) /* > op only valid for int, else runtime error */
					{
						int rhsINT = atoi(query.conditions[0].RHS);
						int lhsINT = atoi(pch);
						if(lhsINT>rhsINT)
						{
							copy=1;
						}
					}
					else if(strcmp(query.conditions[0].oP,"<")==0) /* > op only valid for int, else runtime error */
					{
						int rhsINT = atoi(query.conditions[0].RHS);
						int lhsINT = atoi(pch);
						if(lhsINT<rhsINT)
						{
							copy=1;
						}
					}
					
				}
				
				//dbTable.columnLen++;
				pch = strtok(NULL, ",");
				colIndex++;
			}
			if(copy==1)
			{
				dbTable.noOfSelectedRows++;
			}			
   		}	
   		dbTable.noOfSelectedCols = colSelectedIndex;
   	}
   	if(VERBOSE){printf("readFromPartitionedDatabase() : Exit\n");}
   	return dbTable;
}

//Just to print struct Query
void printQuery(struct Query query)
{
	printf("printQuery: start\n");
	int i;
	for(i=0;i<query.columnsLen;i++)
	{
		printf("column[%d]: %s,%d\n",i,query.columns[i],query.columnsIndex[i]);
		
	}
	for(i=0;i<query.tablesLen;i++)
	{
		printf("table[%d]: %s\n",i,query.tables[i]);
	}
	for(i=0;i<query.conditionsLen;i++)
	{
		printf("conditions[%d]: %s,%d,%s,%s\n",i,query.conditions[i].txt,query.conditions[i].colIndex,query.conditions[i].oP,query.conditions[i].RHS);
	}
}

//Make the Query according to database
struct Query makeQuery(struct Query query,char* dbName)
{
	if(VERBOSE){printf("makeQuery(): Enter\n");}
	//myQuery.columnsIndex = calloc(MAX_QUERY_COL, sizeof(char*));
	int i;
	char* temp;
	for(i=0;i<query.columnsLen;i++)
	{
		temp = getColumnIndex(query.columns[i],dbName);
		query.columnsIndex[i] = atoi(temp);	
	}
	for(i=0;i<query.conditionsLen;i++)
	{
		char tempColName[MAX_COL_NAME];
		char tempRHS[MAX_DB_STRING];
		int charPos = 0;
		while((query.conditions[i].txt[charPos]!='=')&(query.conditions[i].txt[charPos]!='<')&(query.conditions[i].txt[charPos]!='>')&(query.conditions[i].txt[charPos]!='!'))
		{
			tempColName[charPos] = query.conditions[i].txt[charPos];
			charPos++;
		}
		tempColName[charPos] = '\0';
		query.conditions[i].colIndex = atoi(getColumnIndex(tempColName,dbName));
		query.conditions[i].oP[0] = query.conditions[i].txt[charPos];
		charPos++;
		if(query.conditions[i].txt[charPos]=='=')
		{
			query.conditions[i].oP[1] = '=';
			query.conditions[i].oP[2] = '\0';
			charPos++;
		}
		else
		{
			query.conditions[i].oP[1] = '\0';
		}
		int j=0;
		while((query.conditions[i].txt[charPos]!='\n')&&(query.conditions[i].txt[charPos]!='\0'))
		{
			tempRHS[j] = query.conditions[i].txt[charPos];
			charPos++;
			j++;
		}
		tempRHS[j]='\0';
		strcpy(query.conditions[i].RHS,tempRHS);
	}
	
	if(VERBOSE){printf("makeQuery() : exit\n");}
	
	return query;
}

//This function takes a queryString as input and return a struct
struct Query parseQuery(char* queryString)
{
	struct Query query;
	//query.tables = calloc(MAX_QUERY_TABLE, sizeof(char*));
	query.columns = calloc(MAX_QUERY_COL, sizeof(char*));
	char** args = calloc(100, sizeof(char*));
	char * pch;	
	//if(VERBOSE){printf (" In parseQuery: Splitting query \"%s\" into tokens:\n",queryString);}
	
	//pos=0 initially, 1 after select, 2 after from, 3 after where
	int i=0,pos=0;
	
	//this will extract the first substring from starting of queryString till it finds a space or comma
	pch = strtok (queryString," ,");
	query.columnsLen = 0;
	query.tablesLen = 0;
	query.conditionsLen = 0;
	while (pch != NULL)
	{
		if((pos==0)&&((strcmp(pch,"SELECT")==0)||(strcmp(pch,"select")==0)))
		{
			//if(VERBOSE)printf("Select Found\n");
			pos=1;
			query.columnsLen = 0;
		}
		else if((pos==1)&&((strcmp(pch,"FROM")==0)||(strcmp(pch,"from")==0)))
		{
			//if(VERBOSE)printf("From Found\n");
			pos=2;
			query.tablesLen = 0;
		}
		else if((pos==2)&&((strcmp(pch,"WHERE")==0)||(strcmp(pch,"where")==0)))
		{
			//if(VERBOSE)printf("Where Found\n");
			pos=3;
			query.conditionsLen = 0;
		}
		else /* If SELECT, FROM or WHERE wasn't found then it must be in between */
		{
			if(pos==1)
			{
				//if(VERBOSE){printf("pos = 1,columnsLen=%d\n",query.columnsLen);}
				query.columns[query.columnsLen]=pch;
				//if(VERBOSE){printf("copied %s to columns[%d] - %s\n",pch,query.columnsLen,query.columns[query.columnsLen]);}
				query.columnsLen++;
			}
			else if(pos==2)
			{
				//if(VERBOSE){printf("pos = 2,tablesLen=%d\n",query.tablesLen);}
				strcpy(query.tables[query.tablesLen],pch);
				if(query.tablesLen==0)
				{
					strcat(query.tables[query.tablesLen],"-");
					strcat(query.tables[query.tablesLen],size);
				}
				//if(VERBOSE){printf("copied %s to tables[%d] - %s\n",pch,query.tablesLen,query.tables[query.tablesLen]);}
				query.tablesLen++;
			}
			else if(pos==3)
			{
				//if(VERBOSE){printf("pos = 3,conditionsLen=%d\n",query.conditionsLen);}
				query.conditions[query.conditionsLen].txt=pch;
				//if(VERBOSE){printf("copied %s to conditions[%d] - %s\n",pch,query.conditionsLen,query.conditions[query.conditionsLen].txt);}
				query.conditionsLen++;
				
				
			}
		}		
		i++;	
		//strtok extracts the next token, this is a subsequent call.. the definition wanted us to give NULL.	
		pch = strtok (NULL, " ,");
		
	}
	return query;
} 

/* his function will read the complete table and return it. No Conditions Apply */
struct DataTable readCompleteTable(char* tableName,char* dbname,int maxRows)
{
	struct DataTable dbTable;
	dbTable.noOfSelectedRows = 0;
	dbTable.noOfSelectedCols = 0;
	dbTable.selectedRows = (row *)malloc(maxRows*sizeof(row));
	char buff[MAX_BUFF_LEN];
	char filePath[MAX_FILEPATH_LEN];
	strcpy(filePath,dbname);
	strcat(filePath,"/");
	strcat(filePath,tableName);
	if(VERBOSE){printf("readCompleteTable() file_path=%s\n",filePath);}
	FILE *fp;
	fp = fopen(filePath, "r");
   	if(fp==NULL)
   	{
   		printf("readCompleteTable() returning, ERROR:Could not read file %s\n",filePath);
   		return;
   	}	
   	else
   	{
   		char* pch;	
   		int colSelectedIndex = 0;
   		while(fscanf(fp, "%s", buff)!=EOF)
   		{
   			colSelectedIndex = 0;
   			int copy=0;
   			//printf("\n%s\t", buff );
   			pch = strtok (buff,",");
   			while(pch != NULL)
			{
				strcpy(dbTable.selectedRows[dbTable.noOfSelectedRows].data[colSelectedIndex],pch);
				colSelectedIndex++;
				pch = strtok(NULL, ",");
			}
			dbTable.noOfSelectedRows++;			
   		}	
   		dbTable.noOfSelectedCols = colSelectedIndex;
   	}
   	if(VERBOSE){printf("readCompleteTable() : Exit\n");}
   	return dbTable;
}

struct DataTable joinDataTables(struct DataTable primary,struct DataTable secondary,struct Query query,char *dbName)
{
	if(VERBOSE){printf("joinDataTables(): Enter\n");}
	//printDataTable(primary);
	//printDataTable(secondary);
	char* secColIndex;
	secColIndex = getColumnIndex(query.conditions[0].RHS,dbName);
	int secondaryColIndex = atoi(secColIndex);
	int primaryColIndex = query.conditions[0].colIndex;
	//printf("PRIMARY:%d, SECONDARY:%d",primaryColIndex,secondaryColIndex);
	struct DataTable joinedDT;
	joinedDT.selectedRows = (row *)malloc(MAX_TABLE_ROWS*sizeof(row));
	int i,j,rowCntIndex=0;
	int colCountIndex=0;
	for(i=0;i<primary.noOfSelectedRows;i++)
	{
		for(j=0;j<secondary.noOfSelectedRows;j++)
		{
			if(strcmp(primary.selectedRows[i].data[primaryColIndex],secondary.selectedRows[j].data[secondaryColIndex])==0)
			{
				
				colCountIndex=0;
				int cnt=0;
				for(cnt=0;cnt<primary.noOfSelectedCols;cnt++)
				{
					strcpy(joinedDT.selectedRows[rowCntIndex].data[colCountIndex],primary.selectedRows[i].data[cnt]);
					colCountIndex++;
				}
				for(cnt=0;cnt<secondary.noOfSelectedCols;cnt++)
				{
					strcpy(joinedDT.selectedRows[rowCntIndex].data[colCountIndex],secondary.selectedRows[j].data[cnt]);
					colCountIndex++;
				}
				rowCntIndex++;
			}
		}
	}
	joinedDT.noOfSelectedRows = rowCntIndex;
	joinedDT.noOfSelectedCols = colCountIndex;
	if(VERBOSE){printf("joinDataTables(): Exit\n");}
	return joinedDT;
}
int main(int argc, char** argv) {
	
	int console=0;
	clock_t begin, end;
	double time_spent;
	begin = clock();	
	//if -v then output whatever the code is doing
	if((argc > 1)&&(strcmp(argv[1],"-console")==0)&&(strcmp(argv[2],"-size")==0))
	{
		strcpy(size,argv[3]);
		console = 1;
	}
	else if((argc > 1)&&(strcmp(argv[1],"-size")==0))
	{
		strcpy(size,argv[2]);
	}
	else
	{
		printf("ERROR: Please Provide -size as 10K 100K 1000K 6000K or 10M");
		return;
	}

	
	//Setting up the environment, rank and size
	MPI_Init(NULL, NULL);
	int world_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	int world_size;
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);
	
	//Creating a string datatype to send rows
	MPI_Datatype MY_STRINGTYPE;
	MPI_Type_contiguous(MAX_TABLE_WORD*MAX_TABLE_COLS, MPI_CHAR, &MY_STRINGTYPE);
	MPI_Type_commit(&MY_STRINGTYPE);
	
	// We are assuming at least 2 processes for this task
	if (world_size < 2) {
	fprintf(stderr, "World size is %d must be greater than 1 for %s\n",world_size, argv[0]);
	MPI_Abort(MPI_COMM_WORLD, 1); 
	}
	
	//Actual code starts
	
	int number;
	char* s;
	int bytes_read = 1;
	size_t nbytes = 10;
	char *my_string;
	int length;
	struct Query myQuery;
	char dbName[1000];
	//strcpy(dbName,DBPATH);
	strcpy(dbName,"");
	sprintf(dbName,"%s%d/DATABASE_TEST_NODE%d",DBPATH,world_size,world_rank);
	//rank=0 process will take input from stdin and send to everyone
	if (world_rank == 0) 
	{
		my_string = (char *)malloc(nbytes+1);
		if(VERBOSE){puts("Please enter the SQL Query");}
		bytes_read = getline(&my_string, &nbytes, stdin);
		if(VERBOSE)
		{
			if (bytes_read == -1)puts ("ERROR!");
			else{puts ("You typed:");puts (my_string);}
		}
		
		length = strlen(my_string);
		
		//First broadcast an INT which is the length of the data which will be sent, then broadcast the data
		MPI_Bcast(&length,1,MPI_INT,0,MPI_COMM_WORLD);
		MPI_Bcast(my_string,length+1,MPI_CHAR,0,MPI_COMM_WORLD);
		myQuery = parseQuery(my_string);
		
		
	}
	else /* everyone else will receive the broadcast sent by rank zero */
	{
		if(VERBOSE){printf("%d Slave process is waiting\n",world_rank);}
		MPI_Bcast(&length,1,MPI_INT,0,MPI_COMM_WORLD);
		char* rec_buf;
		rec_buf = (char *) malloc(length+1);
		MPI_Bcast(rec_buf,length+1,MPI_CHAR,0,MPI_COMM_WORLD);
		if(VERBOSE){printf("%d Slave received:",world_rank);puts(rec_buf);}
		myQuery = parseQuery(rec_buf);
		
	}
	
	/* Now all process have myQuery correctly set*/
	//if (world_rank == 0) {printQuery(myQuery);}
	myQuery = makeQuery(myQuery,dbName);
	//if (world_rank == 0) {printQuery(myQuery);}
	
	/*Now all processes have made the Query according to the metadata file*/
		
	if(myQuery.tablesLen == 1) /* If tablesLen = 1 then it is a SIMPLE SELECT query */
	{
		if(world_rank==0) /* rank=0 is the master, will get all the data and print */
		{
			struct DataTable dbTable = selectFromPartitionedDatabase(myQuery,dbName,MAX_TABLE_ROWS*2);
			//printDataTable(dbTable);
			int i=0;
			for(i=1;i<world_size;i++)
			{
				int noOfRows = 0;
				MPI_Recv(&noOfRows, /* To tell the rank=0 process how many rows it will get next*/
						1, MPI_INT, /*1 integer*/
						i, SELECTED_ROWS_NO_TAG,  /* partner rank and tag*/
						MPI_COMM_WORLD,MPI_STATUS_IGNORE);
	
				MPI_Recv(&dbTable.selectedRows[dbTable.noOfSelectedRows], /* the address after the last row*/
						noOfRows+1,MY_STRINGTYPE, 
						i,SELECT_SEND_TAG, /* partner rank and tag*/
						MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		
				dbTable.noOfSelectedRows += noOfRows;
			}
			if(console==1)
				printDataTable(dbTable);
			printDataTableToFile(dbTable);
		}
		else /* all other processors run the query on their database and send the results to rank=0 */
		{
			
			/*TODO: REMOVE THIS CODE WHEN RUNNING ON REAL MPI CLUSTER 
			if(world_rank == 1)
			strcpy(dbName,"TEST1");
			else
			strcpy(dbName,"TEST2");
			TODO: REMOVE ENDS*/
			
			struct DataTable dbTable = selectFromPartitionedDatabase(myQuery,dbName,MAX_TABLE_ROWS);
			//printDataTable(dbTable);
			MPI_Send(&(dbTable.noOfSelectedRows),1,MPI_INT,0,SELECTED_ROWS_NO_TAG,MPI_COMM_WORLD); /*first send number of rows*/
			MPI_Send(dbTable.selectedRows,dbTable.noOfSelectedRows,MY_STRINGTYPE,0,SELECT_SEND_TAG,MPI_COMM_WORLD); 
		}
	}
	else /* If it is a join query */
	{
		
		
		/*Just Initialising by datastructures */
		int forRank,fromRank;
		struct DataTable myTablePrimary;
		struct DataTable myTableSecondary;
		myTableSecondary.selectedRows = (row *)malloc(2*MAX_TABLE_ROWS*sizeof(row));
		struct DataTable* partnerSecondaryTables;
		partnerSecondaryTables = (struct DataTable*) malloc(sizeof(struct DataTable));
		
		/*TODO: REMOVE THIS CODE WHEN RUNNING ON REAL MPI CLUSTER AND THE ABOVE 0 TOO!
		if(world_rank == 1)
		strcpy(dbName,"TEST1");
		else if(world_rank == 2)
		strcpy(dbName,"TEST2");
		TODO: REMOVE ENDS*/
		
		
		/* Read the complete primary table and partition the secondary table*/
		myTablePrimary = readCompleteTable(myQuery.tables[0],dbName,MAX_TABLE_ROWS);
		//if(world_rank==0)printDataTable(myTablePrimary);
		
		partnerSecondaryTables = readPartnerTablesForJoin(myQuery,dbName,world_size,MAX_JOIN_ROWS);
		
		myTableSecondary = partnerSecondaryTables[world_rank];
		//printDataTable(partnerSecondaryTables[1]);
		
		//if(world_rank==0){printDataTable(partnerSecondaryTables[1]);}
		/* Send the Partition Tables to whoever deserves them. Free whatever you send from your own memory*/
		for(forRank=0;forRank<world_size;forRank++)
		{
			if(forRank!=world_rank)
			{
				MPI_Send(&(partnerSecondaryTables[forRank].noOfSelectedRows),
							1,MPI_INT,
							forRank,SELECTED_JOIN_NO_TAG,
							MPI_COMM_WORLD); /*first send number of rows*/
						
				MPI_Send(partnerSecondaryTables[forRank].selectedRows,
							partnerSecondaryTables[forRank].noOfSelectedRows,MY_STRINGTYPE,
							forRank,JOIN_ROWS_TAG,
							MPI_COMM_WORLD); 
				
			}		
		}
		//printf("rank=%d SENT.................\n",world_rank);
		/* Receive the tables in my share from everyone around the world*/
		for(fromRank=0;fromRank<world_size;fromRank++)
		{
			
			if(fromRank!=world_rank)
			{
				
				int rowsAboutToGet;
				MPI_Recv(&rowsAboutToGet, /* To tell the receiving process how many rows it will get next*/
					1, MPI_INT, /*1 integer*/
					fromRank, SELECTED_JOIN_NO_TAG,  /* partner rank and tag*/
					MPI_COMM_WORLD,MPI_STATUS_IGNORE);
				
				MPI_Recv(&(myTableSecondary.selectedRows[myTableSecondary.noOfSelectedRows]), /* the address after the last row*/
					rowsAboutToGet+1,MY_STRINGTYPE, 
					fromRank,JOIN_ROWS_TAG, /* partner rank and tag*/
					MPI_COMM_WORLD,MPI_STATUS_IGNORE);
				
				myTableSecondary.noOfSelectedRows += rowsAboutToGet;
				//printf("<<<<<%d<<<<<< %d received %d from %d\n",myTableSecondary.noOfSelectedRows,world_rank,rowsAboutToGet,fromRank);
			}
			
		}
		//printf("rank=%d RECV...................",world_rank);
		/* JOIN whatever I need to join */
		//if(world_rank==0)
		//{
		struct DataTable joinedTable;
		joinedTable = joinDataTables(myTablePrimary,myTableSecondary,myQuery,dbName);
		free(myTablePrimary.selectedRows);
		free(myTableSecondary.selectedRows);
		//printDataTable(joinedTable);
		//}
		
		
		
		if(world_rank==0)/* Master node rank=0 collects all the data and displays it*/
		{
			int i=0;
			for(i=1;i<world_size;i++)
			{
				int noOfRows = 0;
				MPI_Recv(&noOfRows, /* To tell the rank=0 process how many rows it will get next*/
						1, MPI_INT, /*1 integer*/
						i, SELECTED_ROWS_NO_TAG,  /* partner rank and tag*/
						MPI_COMM_WORLD,MPI_STATUS_IGNORE);
	
				MPI_Recv(&joinedTable.selectedRows[joinedTable.noOfSelectedRows], /* the address after the last row*/
						noOfRows+1,MY_STRINGTYPE, 
						i,SELECT_SEND_TAG, /* partner rank and tag*/
						MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		
				joinedTable.noOfSelectedRows += noOfRows;
			}
			if(console==1)
				printDataTable(joinedTable);
			printDataTableToFile(joinedTable);
		}
		else/* Send the data to master world_rank=0 to display*/
		{
			MPI_Send(&(joinedTable.noOfSelectedRows),1,MPI_INT,0,SELECTED_ROWS_NO_TAG,MPI_COMM_WORLD); /*first send number of rows*/
			MPI_Send(joinedTable.selectedRows,joinedTable.noOfSelectedRows,MY_STRINGTYPE,0,SELECT_SEND_TAG,MPI_COMM_WORLD); 
		}
	}
		
	if(world_rank == 0)
	{
		end = clock();
		time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
		printf("\nThe Query took: %lf s\n",time_spent);
	}
	MPI_Finalize();
}
