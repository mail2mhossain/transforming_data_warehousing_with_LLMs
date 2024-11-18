import os
import json
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, joinedload


# Define the project root and the path to the database file
project_root = os.path.abspath(os.path.dirname(__file__) + "/..")
db_path = os.path.join(project_root, "data", "db_info.db")

# Ensure the `data` directory exists
os.makedirs(os.path.dirname(db_path), exist_ok=True)

# Define the DATABASE_URL and create the engine
DATABASE_URL = f"sqlite:///{db_path}"
engine = create_engine(DATABASE_URL, echo=True)


# Define a base class for our classes to inherit from
Base = declarative_base()

# Define the FileInfo model class
class DBInfo(Base):
    __tablename__ = "db_info"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_name = Column(String, nullable=False, unique=True)
    duck_db_path = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    column_descriptions = Column(String, nullable=True)
    df_head = Column(String, nullable=True)
    # Establish a relationship with DBInfoDetails
    details = relationship("DBInfoDetails", back_populates="db_info", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<DBInfo(name='{self.name}', duck_db_path='{self.duck_db_path}', column_description='{self.column_description}')>"

class DBInfoDetails(Base):
    __tablename__ = "db_info_details"

    id = Column(Integer, primary_key=True, autoincrement=True)
    parquet_file_name = Column(String, nullable=False)
    total_rows = Column(Integer, nullable=False)
    offset = Column(Integer, nullable=False)
    chunk_size = Column(Integer, nullable=False)
    # Foreign key reference to DBInfo
    db_info_id = Column(Integer, ForeignKey("db_info.id", ondelete="CASCADE"), nullable=False)
    
    # Establish a relationship with DBInfo
    db_info = relationship("DBInfo", back_populates="details")

    def __repr__(self):
        return f"<DBInfoDetails(parquet_file_path='{self.parquet_file_name}', db_info_id='{self.db_info_id}')>"

# Create the file_info table
Base.metadata.create_all(engine)

# Create a sessionmaker bound to the engine
Session = sessionmaker(bind=engine)

# Function to insert data into the table
def insert_file_info(dataset_name, duck_db_path, table_name, column_descriptions, df_head):
    session = Session()
    try:
        # Check if a DBInfo with the given name already exists
        existing_file = session.query(DBInfo).filter(func.lower(DBInfo.dataset_name) == dataset_name.lower()).first()
        if existing_file:
            return json.dumps({"success": False, "message": f"A dataset with the name '{dataset_name}' already exists."})

        # Create the DBInfo record
        file_info = DBInfo(dataset_name=dataset_name, 
                           duck_db_path=duck_db_path, 
                           table_name=table_name, 
                           column_descriptions=column_descriptions, 
                           df_head=df_head)
        
        # Create the DBInfoDetails record and associate it with file_info
        # db_info_detail = DBInfoDetails(parquet_file_name=parquet_file_name)
        # file_info.details.append(db_info_detail)  # Add db_info_detail to the file_info relationship
        
        # Add only DBInfo (file_info); SQLAlchemy will handle DBInfoDetails via the relationship
        session.add(file_info)
        session.commit()
        
        return json.dumps({"success": True, "message": f"Dataset '{dataset_name}' added successfully!"})
    except Exception as e:
        session.rollback()  # Rollback in case of error
        return json.dumps({"success": False, "message": f"Error inserting file info: {e}"})
    finally:
        session.close()

def update_column_description(dataset_name, column_descriptions):
    session = Session()
    try:
        # Check if a FileInfo with the given name already exists
        existing_file = session.query(DBInfo).filter(func.lower(DBInfo.dataset_name) == dataset_name.lower()).first()
        if not existing_file:
            return json.dumps({"success": False, "message": f"A dataset with the name '{dataset_name}' does not exist."})

        # Proceed with insertion if name exist
        existing_file.column_descriptions = column_descriptions
        session.commit()
        return json.dumps({"success": True, "message": f"Column description for '{dataset_name}' updated successfully!"})
    except Exception as e:
        print(f"Error updating column description: {e}")
        return json.dumps({"success": False, "message": f"Error updating column description: {e}"})
    finally:
        session.close()


def insert_db_info_details(dataset_name, parquet_file_name, total_rows, offset, chunk_size):
    session = Session()
    try:
        # Find the DBInfo record by name
        existing_db_info = session.query(DBInfo).filter(func.lower(DBInfo.dataset_name) == dataset_name.lower()).first()
        if not existing_db_info:
            return json.dumps({"success": False, "message": f"No dataset found with the name '{dataset_name}'."})

        existing_detail = session.query(DBInfoDetails).filter(
            DBInfoDetails.parquet_file_name == parquet_file_name,
            DBInfoDetails.db_info_id == existing_db_info.id
        ).first()

        if existing_detail:
            # Update the existing DBInfoDetails record
            existing_detail.offset = offset
            message = f"Parquet file '{parquet_file_name}' updated in '{dataset_name}' successfully!"
        else:
            # Create a new DBInfoDetails record and associate it with the existing DBInfo record
            new_db_info_detail = DBInfoDetails(
                parquet_file_name=parquet_file_name,
                total_rows=total_rows,
                offset=offset,
                chunk_size=chunk_size,
                db_info=existing_db_info
            )
            session.add(new_db_info_detail)
            message = f"Parquet file '{parquet_file_name}' added to '{dataset_name}' successfully!"

        session.commit()
        return json.dumps({"success": True, "message": message})
    except Exception as e:
        return json.dumps({"success": False, "message": f"Error inserting DBInfoDetails: {e}"})
    finally:
        session.close()


def if_dataset_exist(dataset_name):
    session = Session()
    try:
        existing_file = session.query(DBInfo).filter(func.lower(DBInfo.dataset_name) == dataset_name.lower()).first()
        if existing_file:
            return True
        else:
            return False
    except Exception as e:
        return False
    finally:
        session.close()


def get_db_info_by_dataset(dataset_name):
    session = Session()
    try:
        existing_file = session.query(DBInfo).filter(func.lower(DBInfo.dataset_name) == dataset_name.lower()).first()
        return existing_file
    finally:
        session.close()


def get_all_file_info():
    session = Session()
    try:
        # Use joinedload to eagerly load the related DBInfoDetails records
        files = session.query(DBInfo).options(joinedload(DBInfo.details)).all()

        # Construct the output list with DBInfo and associated DBInfoDetails data
        result = []
        for file in files:
            db_info_data = {
                "name": file.dataset_name,
                "duck_db_path": file.duck_db_path,
                "table_name": file.table_name,
                "column_descriptions": file.column_descriptions,
                "df_head": file.df_head,
                "details": [
                    {"parquet_file_name": detail.parquet_file_name, "db_info_id": detail.db_info_id}
                    for detail in file.details
                ]
            }
            result.append(db_info_data)

        return result
    finally:
        session.close()


def is_file_loaded(dataset_name: str, parquet_file_path: str) -> str:
    session = Session()
    parquet_file_name = os.path.basename(parquet_file_path)
    try:
        db_info = session.query(DBInfo).filter_by(dataset_name=dataset_name).first()
        
        if not db_info:
            return {"success": False, 
                    "message": f"Dataset '{dataset_name}' not found.", 
                    "offset": 0,
                    "progress": 0.0}
        
        # Retrieve the DBInfoDetails entry for the given parquet_file_name and db_info_id
        details = session.query(DBInfoDetails).filter_by(parquet_file_name=parquet_file_name, db_info_id=db_info.id).first()
        
        if not details:
            return {"success": False, 
                    "message": f"Parquet file '{parquet_file_name}' not found for dataset '{dataset_name}'.", 
                    "offset": 0,
                    "progress": 0.0}

        # Check if the file is fully loaded or partially loaded
        if details.offset + details.chunk_size >= details.total_rows:
            return {"success": True, 
                    "message": f"Parquet file '{parquet_file_name}' is loaded for dataset '{dataset_name}'.", 
                    "offset": details.offset + details.chunk_size,
                    "progress": 1.0}
        else:          
            progress = min((details.offset + details.chunk_size) / details.total_rows, 1.0)
            # print(f"Progress:{progress}")
            return {"success": False, 
                    "message": f"Parquet file '{parquet_file_name}' is partially loaded for dataset '{dataset_name}'.", 
                    "offset": details.offset + details.chunk_size,
                    "progress": progress}
    finally:
        session.close()