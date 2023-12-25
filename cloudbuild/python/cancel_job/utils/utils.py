"""Python class containing basic utilities required for the input intake.
"""
import re


class ArgumentInputUtils:
    """Class for command like argument intake and validation."""

    def __init__(
        self,
        argv,
        required_arguments,
        acceptable_arguments,
    ):
        self.argv = argv
        self.required_arguments = required_arguments
        self.acceptable_arguments = acceptable_arguments

    def __get_arguments(self):
        """Parse the command line arguments and store them in a dictionary.

        Returns:
          A dictionary {argument_name: argument_value}.
            Where argument_name is the name of the argument provided.
            argument_value as the value for the concerned named argument.

        """
        # Arguments are provided of the form "--argument_name=argument_value"
        # We need to extract the name and value as a part of a dictionary.
        # i.e
        # {argument1_name: argument1_value, argument2_name: argument2_value, ...}
        # containing all arguments
        # The pattern
        #     --(\w+)=(.*): Searches for the exact '--'
        #         followed by a group of word character (alphanumeric & underscore)
        #         then an '=' sign
        #         followed by any character except linebreaks
        argument_pattern = r'--(\w+)=(.*)'

        # Forming a dictionary from the arguments
        try:
            matches = [
                re.match(argument_pattern, argument) for argument in self.argv[1:]
            ]
            argument_dictionary = {
                match.group(1): match.group(2) for match in matches
            }
            del matches
        except AttributeError as exc:
            raise UserWarning(
                '[Log: parse_logs ERROR] Missing argument. Please check the arguments'
                ' provided again.'
            ) from exc
        return argument_dictionary

    def __validate_arguments(self, arguments_dictionary):
        for required_argument in self.required_arguments:
            if required_argument not in arguments_dictionary:
                raise UserWarning(
                    f'[Log: parse_logs ERROR] {required_argument} argument not provided'
                )
        for key, _ in arguments_dictionary.items():
            if key not in self.acceptable_arguments:
                raise UserWarning(
                    f'[Log: parse_logs ERROR] Invalid argument "{key}" provided'
                )

    def input_validate_and_return_arguments(self):
        arguments_dictionary = self.__get_arguments()
        self.__validate_arguments(arguments_dictionary)
        return arguments_dictionary
