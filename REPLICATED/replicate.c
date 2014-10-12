//Asumptions:
//1. At least 1 node will have both the tables to join
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#define MAX_NODES 8
#define MAX_TABLES 100
#define MAX_TABLENAME 15
#define DBPATH ""
#define MAX_CONDITIONS 10
#define MAX_QUERY_COL 10
#define MAX_QUERY_TABLE 100
#define MAX_FILEPATH_LEN 1000
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
#define MAX_TABLE_WORD 15 //NOTE:If this is not set right then high probability of SEGMENTATION FAULT
int VERBOSE = 0;
typedef struct
{
	int noOfTables;
	char tableName[MAX_TABLES][MAX_TABLENAME]; /* Name of the Table 'i' */
	int nodeNumbers[MAX_TABLES][MAX_NODES]; /* Exists on Nodes [i][0],[i][1] etc */
} mapping;
typedef struct Condition
{
	char* txt;
	int colIndex;
	char oP[5];
	char RHS[50];
}condition;
struct Query
{
	char** tables;
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
//This function prints the DataTable struct to the console
//WARNING: If the file is very large using this function might keep printing the DataTable for a long time.
void printDataTable(struct DataTable dbTable)
{
	int i,j;
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
//This function prints the DataTable to a file named OUTPUT in the present directory
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
/* This function will print the mapping structure nicely*/
void printMapping(mapping repMap)
{
	printf("printing Map");
	int i,j;
	for(i=0;i<repMap.noOfTables;i++)
	{
		printf("%s: ",repMap.tableName[i]);
		for(j=0;j<MAX_NODES;j++)
		{
			printf("%d ",repMap.nodeNumbers[i][j]);
		}
		printf("\n");
	}
}


int checkIfNodeInMapping(mapping repMap,char* tableName,int Node)
{
	printf("printing Map");
	int i,j;
	for(i=0;i<repMap.noOfTables;i++)
	{
		if(strcmp(repMap.tableName[i],tableName)==0)
		{
			for(j=0;j<MAX_NODES;j++)
			{
				if(repMap.nodeNumbers[i][j]==0)
				{
					return 1;
				}
			}
		}
	}
	return 0;
}
/* get Mapping */
mapping getMapping(char* filePath)
{
	
	mapping replicaMap;
	replicaMap.noOfTables = 0;
	char buff[MAX_BUFF_LEN];
	FILE *fp;
	fp = fopen(filePath, "r");
   	if(fp==NULL)
   	{
   		printf("getMapping() returning, ERROR:Could not read file %s",filePath);
   		return;
   	}	
   	else
   	{
   		char* pch;
   		while(fscanf(fp, "%s", buff)!=EOF)
   		{
   			pch = strtok (buff,",");
   			strcpy(replicaMap.tableName[replicaMap.noOfTables],pch);
   			pch = strtok(NULL, ",");
   			int nodes=0;
   			while(pch != NULL)
			{
				int nodeNumber = atoi(pch);
				replicaMap.nodeNumbers[replicaMap.noOfTables][nodes++] = nodeNumber;
				pch = strtok(NULL, ",");
			}
			int i;
			for(i=nodes;i<MAX_NODES;i++)
			{
				replicaMap.nodeNumbers[replicaMap.noOfTables][i] = -1;
			}
			replicaMap.noOfTables++;
   		}
   	}
   	return replicaMap;
   	
}


//This function takes a queryString as input and return a struct
struct Query parseQuery(char* queryString)
{
	struct Query query;
	query.tables = calloc(MAX_QUERY_TABLE, sizeof(char*));
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
				query.tables[query.tablesLen]=pch;
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
int getNoOfLines(char* filePath)
{
	FILE *fp;
	fp = fopen(filePath, "r");
	int ch;
	int count=0;
	if(fp==NULL)
   	{
   		printf("getNoOfLines() returning, ERROR:Could not read file %s",filePath);
   		return;
   	}	
	do
	{
		ch = fgetc(fp);
		if( ch== '\n') count++;   
	}while( ch != EOF );   
	return count; 
}

//gets the start index for the given table and processor
setIndexForJoin(int index[],mapping repMap,int world_rank,char* databasePath,char* tableName,char* tableName2)
{
	if(VERBOSE){printf("setIndexForJoin(): Enter");}
	
	char filePath[MAX_FILEPATH_LEN];
	strcpy(filePath,databasePath);
	strcat(filePath,"/");
	strcat(filePath,tableName);
	int i,j,table2Index,table1Index,totalCount=0,present=-1;
	for(i=0;i<repMap.noOfTables;i++)
	{
		if(strcmp(repMap.tableName[i],tableName2)==0)
		{
			table2Index = i;
		}
	}
	for(i=0;i<repMap.noOfTables;i++)
	{
		if(strcmp(repMap.tableName[i],tableName)==0)
		{
			table1Index = i;
		}
	}
	for(i=0;(i<MAX_NODES)&&(repMap.nodeNumbers[table1Index][i]!=-1);i++)
	{
		int j;
		for(j=0;(j<MAX_NODES)&&(repMap.nodeNumbers[table2Index][j]!=-1);j++)
		{
			if(repMap.nodeNumbers[table1Index][i]==repMap.nodeNumbers[table2Index][j])
			{
				
				if(repMap.nodeNumbers[table1Index][i]==world_rank)
				{
				
					present = totalCount;
				}
				totalCount++;
			}
		}
	}
	if(present != -1)
	{
		int total=getNoOfLines(filePath);
		if(VERBOSE){
			printf("%s is with %d nodes has %d lines -- \n",tableName,totalCount,total);
		}
		index[0] = (total/totalCount)*present;
		index[1] = ((total/totalCount)*(present+1))-1;
	}
	else
	{
		index[0] = 0;
		index[1] = 0;
	}
	if(VERBOSE){
		printf("\n--------------------rank=%d got %d %d\n",world_rank,index[0],index[1]);
	}
	if(VERBOSE){printf("setIndexForJoin(): Exit");}
	return;
}

//gets the start index for the given table and processor
setIndex(int index[],mapping repMap,int world_rank,char* databasePath,char* tableName)
{
	if(VERBOSE){printf("setIndex(): Enter");}
	
	char filePath[MAX_FILEPATH_LEN];
	strcpy(filePath,databasePath);
	strcat(filePath,"/");
	strcat(filePath,tableName);
	int i,j;
	for(i=0;i<repMap.noOfTables;i++)
	{
		if(strcmp(repMap.tableName[i],tableName)==0)
		{
			int division=0,nodeInMapping=-1;
			for(j=0;(j<MAX_NODES)&&(repMap.nodeNumbers[i][j]!=-1);j++)
			{
				if(repMap.nodeNumbers[i][j]==world_rank)
					nodeInMapping = j;
				division++;
			}
			if(nodeInMapping != -1)
			{
				int total=getNoOfLines(filePath);
				if(VERBOSE){
					printf("%s is with %d nodes has %d lines -- \n",tableName,division,total);
				}
				index[0] = (total/division)*nodeInMapping;
				index[1] = ((total/division)*(nodeInMapping+1))-1;
			}
			else
			{
				index[0] = 0;
				index[1] = 0;
			}
			if(VERBOSE){
					printf("\n--------------------rank=%d got %d %d\n",world_rank,index[0],index[1]);
			}
			if(VERBOSE){printf("setIndex(): Exit");}
			return;
			
		}
	}
	if(VERBOSE){printf("setIndex(): WARNING.. Incorrect Exit!");}
	return;
}


//Reads the METADATA file to get the ColumnIndex
char* getColumnIndex(char* columnName,char* dbName)
{
	char databasePath[MAX_FILEPATH_LEN];
	char buff[MAX_FILEPATH_LEN];
	char filePath[MAX_FILEPATH_LEN];
	int colIndex = -1;
	FILE *fp;
	strcpy(databasePath,DBPATH);
	strcat(databasePath,dbName);
	strcpy(filePath,databasePath);
	strcat(filePath,"/METADATA");
	fp = fopen(filePath, "r");
	if(fp==NULL)
	{
		if(VERBOSE){printf("getColumn() ERROR: Could not Read file: %s\n",filePath);}
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
	fclose(fp);
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

/*This function will read the given Query from the database dbname
Presently supports only a single WHERE A=B condition*/
struct DataTable selectFromReplicatedDatabase(struct Query query,char* dbname,int maxRows,int startIndex,int endIndex)
{
	struct DataTable dbTable;
	dbTable.noOfSelectedRows = 0;
	dbTable.noOfSelectedCols = 0;
	dbTable.selectedRows = (row *)malloc(maxRows*sizeof(row));
	char databasePath[MAX_FILEPATH_LEN];
	char buff[MAX_BUFF_LEN];
	strcpy(databasePath,DBPATH);
	strcat(databasePath,dbname);
	char filePath[MAX_FILEPATH_LEN];
	strcpy(filePath,databasePath);
	strcat(filePath,"/");
	strcat(filePath,query.tables[0]);
	if(VERBOSE){printf("selectFromReplicatedDatabase() file_path=%s\n",filePath);}
	FILE *fp;
	fp = fopen(filePath, "r");
   	if(fp==NULL)
   	{
   		printf("selectFromReplicatedDatabase() returning, ERROR:Could not read file");
   		return;
   	}	
   	else
   	{
   		char* pch;	
   		int colIndex=0;	
   		int colSelectedIndex = 0;
   		int fileLineIndex = 0;
   		while(fscanf(fp, "%s", buff)!=EOF)
   		{
   			if((fileLineIndex>=startIndex)&&(fileLineIndex<=endIndex))
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
			fileLineIndex++;		
   		}	
   		dbTable.noOfSelectedCols = colSelectedIndex;
   	}
   	if(VERBOSE){printf("selectFromReplicatedDatabase() : Exit\n");}
   	return dbTable;
}

/* his function will read the complete table and return it. No Conditions Apply */
struct DataTable readPartialTable(char* tableName,char* dbname,int maxRows,int start,int end)
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
   		int rowNumber = 0;
   		while(fscanf(fp, "%s", buff)!=EOF)
   		{
   			if((rowNumber>=start)&&(rowNumber<=end))
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
			rowNumber++;	
   		}	
   		dbTable.noOfSelectedCols = colSelectedIndex;
   	}
   	if(VERBOSE){printf("readCompleteTable() : Exit\n");}
   	return dbTable;
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
	if(VERBOSE){
	printf("joinDataTables(): Enter\n");
	}
	//printDataTableToFile(primary);
	//printDataTableTo(secondary);
	char* secColIndex;
	secColIndex = getColumnIndex(query.conditions[0].RHS,dbName);
	int secondaryColIndex = atoi(secColIndex);
	int primaryColIndex = query.conditions[0].colIndex;
	if(VERBOSE){printf("PRIMARY:%d, SECONDARY:%d\n",primaryColIndex,secondaryColIndex);}
	struct DataTable joinedDT;
	joinedDT.selectedRows = (row *)malloc(MAX_TABLE_ROWS*sizeof(row));
	int i,j,rowCntIndex=0;
	int colCountIndex=0;
	for(i=0;i<primary.noOfSelectedRows;i++)
	{
		for(j=0;j<secondary.noOfSelectedRows;j++)
		{
		//printf("-.-.-.%s,%s\n",primary.selectedRows[i].data[primaryColIndex],secondary.selectedRows[j].data[secondaryColIndex]);
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
	//printDataTable(joinedDT);
	if(VERBOSE){printf("joinDataTables(): Exit\n");}
	return joinedDT;
}

int main(int argc, char** argv) {
	int console=0;
	clock_t begin, end;
	double time_spent;
	begin = clock();	
	//if -v then output whatever the code is doing
	if((argc > 1)&&(strcmp(argv[1],"-console")==0))
	{
		console = 1;
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

	mapping repMap;
	repMap = getMapping("REPLICATED_MAPPING");
	
	/* Read the Mapping */
	if(world_rank==0)
	{
	//	printMapping(repMap);
	}
	
	/* Distribute the Query */
	int number;
	char* s;
	int bytes_read = 1;
	size_t nbytes = 10;
	char *my_string;
	int length;
	struct Query myQuery;
	char dbName[15];
	sprintf(dbName, "%d", world_rank);
	strcat(dbName,"_REP_DB");
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
	
	/* At this point, everyone has the myQuery object*/
	myQuery = makeQuery(myQuery,dbName);
	
		
	if(myQuery.tablesLen == 1) /* If tablesLen = 1 then it is a SIMPLE SELECT query */
	{
		/*Calculate your Index*/
		int Index[2];
		setIndex(Index,repMap,world_rank,dbName,myQuery.tables[0]);
		if(world_rank==0)
		{
			if(VERBOSE){printf("Starting Select\n");}
			struct DataTable dbTable = selectFromReplicatedDatabase(myQuery,dbName,MAX_TABLE_ROWS*2,Index[0],Index[1]);
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
			else
			printDataTableToFile(dbTable);
		}
		else
		{
			if(VERBOSE){printf("In else block\n");}
			struct DataTable dbTable = selectFromReplicatedDatabase(myQuery,dbName,MAX_TABLE_ROWS,Index[0],Index[1]);
			/*if(world_rank==2)
			{
				printf("%d .. %d",Index[0],Index[1]);
				printDataTable(dbTable);
			}*/
			MPI_Send(&(dbTable.noOfSelectedRows),1,MPI_INT,0,SELECTED_ROWS_NO_TAG,MPI_COMM_WORLD); /*first send number of rows*/
			MPI_Send(dbTable.selectedRows,dbTable.noOfSelectedRows,MY_STRINGTYPE,0,SELECT_SEND_TAG,MPI_COMM_WORLD); 
		}
	}
	else /* Two tables mean JOIN */
	{
		int Index[2];
		setIndexForJoin(Index,repMap,world_rank,dbName,myQuery.tables[0],myQuery.tables[1]);
		struct DataTable joinedTable;
		joinedTable.selectedRows = (row *)malloc(MAX_TABLE_ROWS*sizeof(row));
		joinedTable.noOfSelectedRows = 0;
		
		if((Index[1]!=0)||(Index[0]!=0))
		{
			struct DataTable myTablePrimary;
			struct DataTable myTableSecondary;
			myTablePrimary = readPartialTable(myQuery.tables[0],dbName,MAX_TABLE_ROWS,Index[0],Index[1]);
			myTableSecondary = readCompleteTable(myQuery.tables[1],dbName,MAX_TABLE_ROWS);
			free(joinedTable.selectedRows);
			joinedTable = joinDataTables(myTablePrimary,myTableSecondary,myQuery,dbName);
			//printDataTable(joinedTable);
		}
		
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
				if(noOfRows!=0)
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
			if(joinedTable.noOfSelectedRows!=0)
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
