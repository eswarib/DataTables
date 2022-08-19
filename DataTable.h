/******************************************************************************

Class to simulate a datatable

*******************************************************************************/

#include <iostream>
#include <set>
#include <vector>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <boost/any.hpp>

using namespace std;

enum DataFieldType
{
    Boolean = 1,
    Integer,
    FloatingPoint,
    String
};

class DataField
{
    public:
    DataField(){mType = Boolean; mName = "Boolean";}
    DataField(DataFieldType fieldType,std::string name){mType = fieldType; mName=name;}
    ~DataField(){}

    DataFieldType GetFieldType(){return mType;}
    std::string GetFieldName(){return mName;}
    bool operator==(const DataField& field) const
    {
        return((mType == (const_cast<DataField&>(field)).GetFieldType()) && (mName == (const_cast<DataField&>(field)).GetFieldName()));
        //return true;
    }

    private:
    DataFieldType mType;
    std::string mName;
};

struct DataFieldValue{
    boost::any value;
};


class RowSchema
{
    public:
    RowSchema(){mFields.clear();}
    ~RowSchema(){mFields.clear();}
    void AddDataField(const DataField& field){mFields.push_back(field);}
    void AddDataField(const vector<DataField>& fields){mFields.insert(mFields.end(),fields.begin(),fields.end());}
    vector<DataField> GetDataFields() const {return mFields;}
    
    bool operator==(const RowSchema& schema) const {
        return(std::equal(mFields.begin(), mFields.end(), schema.GetDataFields().begin()));
    }
    
    private:
    //a row schema is nothing but set of fields
    vector<DataField> mFields;        
};


class DataFrame
{
    public:
    DataFrame(){}
    DataFrame(RowSchema* schema):mDataSchema(schema){}
    DataFrame(RowSchema* schema, const vector<DataFieldValue>& fieldVec):mDataSchema(schema),mFieldValues(fieldVec){}
    //copy constructor needs to be added
    DataFrame(const DataFrame& df): mDataSchema(df.mDataSchema),mFieldValues(df.mFieldValues){
//        this->mDataSchema->GetDataFields().insert(this->mDataSchema->GetDataFields().end(),df.mDataSchema->GetDataFields().begin(),df.mDataSchema->GetDataFields().end()) ;
//        this->mFieldValues.insert(this->mFieldValues.end(),(*df).GetDataFieldValues().begin(),(*df).GetDataFieldValues().end());
    }

    ~DataFrame(){ mFieldValues.clear();}

    bool SetSchema(RowSchema* schema);
    
    RowSchema* GetSchema(){return mDataSchema;}

    vector<DataFieldValue> GetDataFieldValues(){return mFieldValues;}

    bool SetOrAddDataValues(vector<DataFieldValue> dfVal){
        mFieldValues.insert(mFieldValues.end(),dfVal.begin(),dfVal.end());
        return true;
    }
    bool AddDataValue(DataFieldValue dfVal){
        mFieldValues.push_back(dfVal);
        return true;
    }
    void printDF();
    private:
    //is a data for a particular RowSchema
    RowSchema* mDataSchema;
    vector<DataFieldValue> mFieldValues; 

};

bool JoinDataFrame(DataFrame& df1,DataFrame& df2,std::string colName,DataFrame* df3)
{
    vector<DataFieldValue> d3FieldValueVec;
    d3FieldValueVec.clear();
    vector<DataFieldValue> dataFieldsVec1 = df1.GetDataFieldValues(); //assuming GetSchema returns valid ptr
    vector<DataFieldValue> dataFieldsVec2 = df2.GetDataFieldValues(); //assuming GetSchema returns valid ptr
    //cout << "dataFieldsVec1 size " << dataFieldsVec1.size() << endl;
    //cout << "dataFieldsVec2 size " << dataFieldsVec2.size() << endl;
    
    d3FieldValueVec.insert(d3FieldValueVec.end(),dataFieldsVec1.begin(), dataFieldsVec1.end());
    d3FieldValueVec.insert(d3FieldValueVec.end(),(++dataFieldsVec2.begin()),dataFieldsVec2.end());
    //cout << "d3FieldValueVec size " << d3FieldValueVec.size() << endl;
    
    df3->SetOrAddDataValues(d3FieldValueVec);

    //cout << "df3 FieldValueVec size " << df3->GetDataFieldValues().size() << endl;
    //cout << "df3 schema size " << df3->GetSchema()->GetDataFields().size() << endl;
    
    
    return true;
}

//first lets write a data table for the given 
//structure then we can generalise
//this class will be templatised
class DataTable
{
    public:
    DataTable(){mData.clear();}
    //read from a csv file
    DataTable(std::string csvFileName,string* headers,RowSchema* rowSchema,int nRows,int nColumns);
    //read from a bulk data
    DataTable(string* headers,DataFrame** data,int nRows,int nColumns);

    ~DataTable(){mData.clear();}
    
    //should be able to add a column to the data table
    bool AddColumn(std::string colName, DataFieldType colType,vector<DataFieldValue> colData);

    //should be able to drop a column
    //Not yet implemented
    bool DropColum(std::string colName){return true;}
    
    bool DropRow(int rowNum){return true;};

    bool DropRow(std::string indexElementName);

    //should be able to drop nRows
    bool DropRows(int* rowNums){return true;} //accepts array of row numbers

    //should be able to add dataframes
    bool AddDataFrame(std::string indexColName,DataFrame& df);
    
    void printDataTable();    
    void printRow(int rowNum){}
    void printColumn(std::string columnBName){}
    void printShape(){cout << "shape of the data table : " << mData.size() << "x" << mColCount << endl;}
    void printHeaders(){}
    
    vector<std::string> GetKeys();
    bool GetDataFrame(std::string rowKey,DataFrame* df);    
    vector<std::string> GetColumnNames();
    
    //This function performs inner join of two datatables having the one common column.
    //other columns are all different
    bool join_datatables(DataTable* d2, std::string colName,DataTable** d3);

    //union of table. This function joins two datatables row wise. The columns
    //of the table should match. No validation done. 
    //schema validation will be added later
    bool union_datatables(DataTable* d2, DataTable** d3);

    //difference of tables. This function get the dataframes unique to each table and puts in the third
    //table row wise. The columns
    //of the table should match. No validation done. 
    //schema validation will be added later
    bool diff_datatables(DataTable* d2, DataTable** d3);

    //intersection of tables. This function get the dataframes common to two tables 
    //and puts in the third table row wise. The columns
    //of the table should match. No validation done. 
    //schema validation will be added later
    bool intersect_datatables(DataTable* d2, DataTable** d3);


    //util functions
    RowSchema* GetSchema(){return mSchema;}

    //get all the dataframes
    std::unordered_map<std::string,DataFrame> GetDataSet(){return mData;}

   friend bool JoinDataFrame(DataFrame& df1,DataFrame& df2,std::string colName,DataFrame* df3);
 
    private:
    
    //set of data
    std::unordered_map<std::string,DataFrame> mData;
    // this is redundant since schema is available in each dataframe
    // but for some function we need to know schema of the data table, so we store again
    RowSchema* mSchema; 
    //std::string* mHeaders;
    int mColCount;

};


static std::string gHeaders1[] = {"molecule_name","solubility","molecular_weight"};
static std::string gHeaders2[] = {"molecule_name","electrical_conductivity","molecular_density"};

const DataFieldType gRowDataTypes[] = {String,FloatingPoint,FloatingPoint};
