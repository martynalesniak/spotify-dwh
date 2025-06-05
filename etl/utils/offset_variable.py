from airflow.models import Variable

def get_file_offset(file_path):
    """Get current offset for a file from Airflow Variables"""
    offsets = Variable.get("file_offsets", default_var={}, deserialize_json=True)
    return offsets.get(file_path, 0)

def update_file_offset(file_path, new_offset):
    """Update offset for a file in Airflow Variables"""
    offsets = Variable.get("file_offsets", default_var={}, deserialize_json=True)
    offsets[file_path] = new_offset
    Variable.set("file_offsets", offsets, serialize_json=True)