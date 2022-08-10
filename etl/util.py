def remove_file_extension(file_name: str) -> str:
    """
    Remove the file extension from a file name.
    
    Parameters
    ----------
    file_name : str
        The file name.
    """

    return file_name.split("/")[-1].split(".")[0]