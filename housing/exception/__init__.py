# Importing required packages
import sys


# Define housing exception custom class
class HousingException(Exception):

    def __int__(self, error_message: Exception, error_detail: sys) -> None:
        super().__init__(error_message)
        self.error_message = HousingException.get_msg_details(error_message, error_detail)

    @staticmethod
    def get_msg_details(error_message: Exception, error_detail: sys):
        _, _, exec_tb = error_detail.exc_info()
        exception_block_line_number = exec_tb.tb_frame.f_lineno
        try_block_line_number = exec_tb.tb_lineno
        file_name = exec_tb.tb_frame.f_code.co_filename
        error_message = f"""
        error occurred in script:
        [{file_name}] at
        try block: [{try_block_line_number}] and
        exception block: [{exception_block_line_number}]
        error message: [{error_message}]
        """
        return error_message

    def __str__(self):
        return self.error_message

    def __repr__(self) -> str:
        return HousingException.__name__.str()