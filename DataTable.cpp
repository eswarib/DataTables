/******************************************************************************
DataTable Class
*******************************************************************************/

#include "DataTable.h"

using namespace std;

DataTable::DataTable(std::string csvFileName,string* headers,RowSchema* rowSchema,int nRows,int nColumns)
//construct datatable from a csv file
{        
    std::string line,word;
    mColCount = nColumns;
    mSchema = rowSchema;
    //lets read csv
    fstream file (csvFileName, ios::in);

    if(file.is_open())
    {
        while(getline(file, line))
        {
            stringstream str(line);
 
            std::vector<DataField>::iterator it = rowSchema->GetDataFields().begin();
            vector<DataFieldValue> fieldVec;
            std::string fieldVecKey = "";

            //cout << "line from csv file : " << line << endl;
            while(getline(str, word, ','))
            {
                if(fieldVecKey == "")
                {
                    fieldVecKey=word;
                }
                //cout << "field : " << word << endl;
                //lets create the fields
                //get the field typede
                //cout << "inserting the fields for df " << endl;
                if(it != rowSchema->GetDataFields().end())
                {
                    DataFieldValue value ;
                    if((it->GetFieldType() == Boolean) ||(it->GetFieldType() == Integer))
                    {                        
                        value.value = stoi(word);
                        //cout << "inserting int value : " << boost::any_cast<int>(value.value);
                    }
                    else if(it->GetFieldType() == FloatingPoint)
                    {
                        value.value = stof(word);
                        //cout << "inserting float value : " << boost::any_cast<float>(value.value);
                    }
                    else if(it->GetFieldType() == Boolean)
                    {
                        value.value = stoi(word);
                    }
                    else if(it->GetFieldType() == String)
                    {
                        value.value = word;
                    }
                    fieldVec.push_back(value);
                    it++;
                }
            }
             //lets build the dataframe
            DataFrame df(rowSchema,fieldVec);
            //cout << "Inserting data frame with key " << fieldVecKey << endl;   
            mData.insert(std::make_pair(fieldVecKey,df));

        }
    }
    else
        cout<<"Could not open the file\n";        
        
}

vector<std::string> DataTable::GetKeys() {
    std::unordered_map<std::string,DataFrame>::iterator it;
    vector<string> keys;
    for(it = mData.begin();it != mData.end(); it++)
    {
        //std::cout << it->first << endl;
        keys.push_back(it->first);
    }
    return keys;
}

bool DataTable::GetDataFrame(std::string rowKey,DataFrame* df){
    std::unordered_map<std::string,DataFrame>::iterator it = mData.find(rowKey);
    if(it != mData.end()){
        *df = it->second;
        return true;
    }
    return false;
}
    
vector<std::string> DataTable::GetColumnNames(){
    vector<DataField> fields = mSchema->GetDataFields();
    vector<std::string> colNames;        
    colNames.clear();
    //get only the names    
    vector<DataField>::iterator it;
    for(it=fields.begin();it != fields.end();it++)
    {
        colNames.push_back((*it).GetFieldName());
    }
    return colNames;
}
 

bool DataTable::AddDataFrame(std::string indexColName,DataFrame& df){
       //cout << "Adding data frame with schema having fields : " << df.GetSchema()->GetDataFields().size() << endl;
       mData.insert(std::make_pair(indexColName,df));
       return true;//as of now this is not used. but in future we may need a status 
    }

 
void DataTable::printDataTable()
{
    //lets print the data tables
//    cout << "no of data frames in the table " << mData.size() << endl;
    for( auto const& iItem: mData ) {
        DataFrame df = iItem.second;
        //cout << "Number of fields in the data frame " << df.GetSchema()->GetDataFields().size() << endl;
        //cout << "Number of field values in the data frame " << df.GetDataFieldValues().size() << endl;
        df.printDF();
        cout << endl;
    }
}

bool DataTable::DropRow(std::string indexElementName)
{
    //first lets check if the element is present
    std::unordered_map<std::string,DataFrame>::iterator it;
    it = mData.find(indexElementName);
    if( it != mData.end())
    {
        //lets remove it
        mData.erase(it);
    }
    return true;
}


bool DataTable::join_datatables(DataTable* d2, std::string colName,DataTable** d3)
 {
     //it is assumed both the tables have same keys (molecule name) but different properties
     //this function does inner join, i.e. if a molecule is found in both d1 and d2, their properties 
     //will be combined and given as a separate table

     //we need to create a new dataframe with a new schema
     //get the column names (headers of each table)
     vector<std::string> headers1 = this->GetColumnNames();
     vector<std::string> headers2 = d2->GetColumnNames();
     

    //lets create a header vector from the about two
    vector<std::string> newHeaders;
    //assuming that only the key element header name (e.g molecule name) is
    //common.
    newHeaders.insert( newHeaders.end(), headers1.begin(), headers1.end() );
    newHeaders.insert( newHeaders.end(), (++headers2.begin()), headers2.end() );
 
    //create new schema
    RowSchema* schema = new RowSchema();
 
    vector<DataField> newDataFieldsVec;
    newDataFieldsVec.clear();
    vector<DataField> dataFieldsVec1 = this->GetSchema()->GetDataFields(); //assuming GetSchema returns valid ptr
    vector<DataField> dataFieldsVec2 = d2->GetSchema()->GetDataFields(); //assuming GetSchema returns valid ptr
    
    newDataFieldsVec.insert(newDataFieldsVec.end(),dataFieldsVec1.begin(), dataFieldsVec1.end());
    newDataFieldsVec.insert(newDataFieldsVec.end(),(++dataFieldsVec2.begin()),dataFieldsVec2.end());
    schema->AddDataField(newDataFieldsVec);

     //get all keys from d1 and d2
     vector<std::string> d1Keys = this->GetKeys();
     vector<std::string> d2keys = d2->GetKeys();

    
    //we need the dataField values
    
    DataFrame df1,df2;
     //for(int i=0;i<d1Keys.size();i++){
    for (auto iItem : d1Keys) {
         //if(d2->GetDataFrame(d1Keys[i],&df2))
         DataFrame df3;
         df3.SetSchema(schema);
         //cout << "item : "<< iItem << endl;
         if(d2->GetDataFrame(iItem,&df2))
         {
             
            if(this->GetDataFrame(iItem,&df1))
            {
                //cout << "d1 index header" << colName << endl;
                //cout << "d1 header 0" << headers1[0] << endl;
                
                bool res = JoinDataFrame(df1,df2,colName,&df3);
                //we need to combine the dataframe and then add
                //cout << "no of fields in the new dataframe after joining : " << df3.GetSchema()->GetDataFields().size() << endl;
                //cout << "no of field values in the new dataframe after joining : " << df3.GetDataFieldValues().size() << endl;
        
                (*d3)->AddDataFrame(iItem,df3);
            }
            
         }
    }
    
     
     return true;
 }


bool DataTable::union_datatables(DataTable* d2, DataTable** d3){

    if(((*mSchema) == (*(d2->GetSchema()))) == false)
    {
        return false;
    }
    //join this table with d2 row wise, duplicates should be eliminated
    for(auto iItem: mData){
        (*d3)->AddDataFrame(iItem.first,iItem.second);
    }
    //since this is an unordered_map internally, same key cannot be added
    //hence duplicates will be eliminated automatically
    for(auto iItem: d2->GetDataSet()){
        (*d3)->AddDataFrame(iItem.first,iItem.second);
    }
    
    return true;
}

bool DataTable::diff_datatables(DataTable* d2, DataTable** d3){

    //get all keys from d1 and d2
    vector<std::string> d1Keys = this->GetKeys();
    vector<std::string> d2keys = d2->GetKeys();

    //lets add all the frames from dt1 to dt3
    for(auto iItem: mData){
        (*d3)->AddDataFrame(iItem.first,iItem.second);
    }

    //now we need to add frames from dt2 one by one while checking
    //if it is already there, in that case remove from dt3
    for(auto iItem: d2->GetDataSet()){
        //check if the key is present in dt1
        if(std::find(d1Keys.begin(),d1Keys.end(),iItem.first)!= d1Keys.end())
        {
            (*d3)->DropRow(iItem.first);//remove frame
        }
        else{
            (*d3)->AddDataFrame(iItem.first,iItem.second);
        }
        
    }    
    return true;
}

bool DataTable::intersect_datatables(DataTable* d2, DataTable** d3)
{
    //get only the keys from d2
    vector<std::string> d2keys = d2->GetKeys();

    for(auto iItem: mData){
        if(std::find(d2keys.begin(),d2keys.end(),iItem.first) != d2keys.end())
        {
            (*d3)->AddDataFrame(iItem.first,iItem.second);
        }
    }
    return true;    
}

bool DataTable::AddColumn(std::string colName, DataFieldType colType,vector<DataFieldValue> colData)
{
    cout << "Entering DataTable::AddColumn" << endl;

    //lets check if the dataframes count and numbe of data in the input colData match
    if(mData.size() != colData.size())
    {
        cout << "the row counts do not match" << endl;
        return false;
    }
    //first we need to update the schema
    DataField field(colType,colName);
    mSchema->AddDataField(field);
    //change the column count
    mColCount = mColCount+1;

    vector<DataFieldValue>::iterator it = colData.begin();
    //now each dataframe need to be updated
    //the algo needs to be improved
    for(auto iItem: mData)
    {
        if(it != colData.end())
        {
            //change the schema
            iItem.second.GetSchema()->AddDataField(field);
            iItem.second.AddDataValue((*it));
            it++;            
        }        
    }
    return true;
}
    

bool DataFrame::SetSchema(RowSchema* schema)
{
    if(schema == NULL)
    {            
        return false;
    }
    //mDataSchema->GetDataFields().insert(mDataSchema->GetDataFields().end(),schema->GetDataFields().begin(),schema->GetDataFields().end());
    mDataSchema = schema;
//    cout << "no of fields in schema : " << schema->GetDataFields().size() << endl;
//    cout << "no of fields in mDataSchema : " << mDataSchema->GetDataFields().size() << endl;
    return true;
}


void DataFrame::printDF()
{
   vector<DataFieldValue>::iterator it;
   vector<DataField> fields = mDataSchema->GetDataFields();
   int size = mFieldValues.size();
   //lets get the values
   //cout << "Number of fields in schema : " << mDataSchema->GetDataFields().size() << endl;

   //cout << "Number of fields in df : " << size << endl;
   for(int i=0; i<size; i++)
   {
       //get the type of the value
       if(fields[i].GetFieldType()==Integer)
       {
          cout << boost::any_cast<int>(mFieldValues[i].value) << "  ";
       }
       else if(fields[i].GetFieldType()==FloatingPoint)
       {
           cout << boost::any_cast<float>(mFieldValues[i].value) << "  ";
       }
       else if(fields[i].GetFieldType()==String)
       {
           cout << boost::any_cast<std::string>(mFieldValues[i].value) << "  ";
       }
       else if(fields[i].GetFieldType()==Boolean)
       {
           cout << boost::any_cast<bool>(mFieldValues[i].value) << "  ";
       }

   }
}


int main()
{
    cout<<"Hello World\n";
    int nSchema1Col = 3,nSchema2Col = 3;
    //lets define the headers
    RowSchema* schema1 = new RowSchema();
    RowSchema* schema2 = new RowSchema();
    
    //as of now these are the fields
    for(int i=0; i<nSchema1Col; i++){
       DataField* field = new DataField(gRowDataTypes[i],gHeaders1[i]);
       schema1->AddDataField(*field);
    }

    //2ns schema
    for(int i=0; i<nSchema2Col; i++){
       DataField* field = new DataField(gRowDataTypes[i],gHeaders2[i]);
       schema2->AddDataField(*field);
    }

    DataTable* dt1_1 = new DataTable("myTable1.csv",&gHeaders1[0],schema1,4,4);
    DataTable* dt1_2 = new DataTable("myTable2.csv",&gHeaders1[0],schema1,4,4);
    DataTable* dt2_1 = new DataTable("myTable3.csv",&gHeaders2[0],schema2,4,4);
    
    DataTable* dt1_1_dup = new DataTable("myTable1.csv",&gHeaders1[0],schema1,4,4);
    

    DataTable* dt3_union = new DataTable();
    //union of tables dt1_1 and dt1_2 - both have same schema
    dt1_1->union_datatables(dt1_2,&dt3_union);
    cout << "-------------------------------------------" << endl;
    cout << " printing dt1_1 " << endl;
    dt1_1->printDataTable();
    cout << endl << " printing dt1_2 " << endl;
    dt1_2->printDataTable();
    cout << endl << "printing dt3_union " << endl;
    dt3_union->printDataTable();

    DataTable* dt3_diff = new DataTable();
    //diff of the tables dt1_1 and dt1_2 - both have same schema
    dt1_1->diff_datatables(dt1_2,&dt3_diff);
    cout << "-------------------------------------------" << endl;
    cout << endl << " printing dt1_1 " << endl;
    dt1_1->printDataTable();
    cout << endl << " printing dt1_2 " << endl;
    dt1_2->printDataTable();
    cout << endl << "printing dt3_diff " << endl;
    dt3_diff->printDataTable();

    DataTable* dt3_intersect = new DataTable();
    //intersection of the tables dt1_1 and dt1_2 - both have same schema
    dt1_1->intersect_datatables(dt1_2,&dt3_intersect);
    cout << "-------------------------------------------" << endl;
    cout << " printing dt1_1 " << endl;
    dt1_1->printDataTable();
    cout << endl << " printing dt1_2 " << endl;
    dt1_2->printDataTable();
    cout << endl << "printing dt3_intersect " << endl;
    dt3_intersect->printDataTable();

    DataTable* dt3_join = new DataTable();
    dt1_1->join_datatables(dt2_1,"molecule_name",&dt3_join);
    cout << "-------------------------------------------" << endl;
    cout << " printing dt1_1 " << endl;
    dt1_1->printDataTable();
    cout << endl << " printing dt2_1 " << endl;
    dt2_1->printDataTable();
    cout << endl << "printing dt3_join " << endl;
    dt3_join->printDataTable();

    delete(dt1_1);
    delete(dt1_2);
    delete(dt2_1);
    delete(dt3_union);
    delete(dt3_diff);
    delete(dt3_intersect);
    delete(dt3_join);
    delete(schema1);
    delete(schema2);
    return 0;
}

