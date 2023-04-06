# Purpose: A program to print to console or write to a file a formatted message
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/15/2023 (htu) - initial coding based on echo_msg from R
#   03/16/2023 (htu) - added test cases
#   03/17/2023 (htu) - removed "\n" from fmt
#   03/30/2023 (htu) - added logic to write to a file 
#
import os
import re

def echo_msg(prg, step, msg, lvl=0, fn=None):
    """
    =========
    echo_msg
    ==========
    
    The echo_msg function is a utility function that prints a formatted 
    message to the console and, optionally, to a log file. It takes the 
    following 
    Parameters:
    -----------
    prg: str
        A string representing the program or module generating the 
        message.
    step: float
        A float or integer indicating the current step or progress
        in the program.
    msg: str
        The message to be printed.
    lvl: int
        An optional integer for the message level (default is 0). 
        Messages with a level less than or equal to the global or debug level
        will be printed.
    fn: str
        An optional string representing the log file's name. If not 
        provided, it will use the log file name specified in the environment
        variable log_fn.

    returns
    -------
        None 

    Raises
    ------
    ValueError
        None
    
    The function first retrieves several environment variables, such as 
    'g_lvl', 'd_lvl', 'log_fn', 'write2log', 'QUERY_STRING', and 'HTTP_HOST'. 
    These variables are used to configure the function's behavior based on 
    the environment.

    The function then processes the msg parameter to hide any passwords that 
    might be included in the message using regular expressions. If the 
    function is being executed in a web environment, it prints the message 
    with appropriate HTML formatting.

    Finally, if the message level lvl is less than or equal to the global or 
    debug level, the message is printed to the console. If the ofn (output 
    file name) and wrt2log (write to log) variables are set, the message is 
    also written to a log file.

    The echo_msg function can be used for logging and debugging purposes, 
    offering both console and file output with different formatting for web 
    and non-web environments.

    """
    fmt = "%s: %.3f - %s"
    f1 = "<h2>%s</h2>"
    f2 = '<font color="%s">%s</font>'
    g_lvl = os.getenv("g_lvl")          # message level
    d_lvl = os.getenv("d_lvl")          # debug level
    logfn = os.getenv("log_fn")         # log file name
    w2log = os.getenv("write2log")    # whether to write to log file: 1 - Yes, 0 - No 
    wrt2log = 0 if w2log is None else int(w2log)
    query_str = os.getenv("QUERY_STRING")
    http_host = os.getenv("HTTP_HOST")
    is_web = bool(query_str and http_host)

    g_lvl = int(g_lvl) if g_lvl else 1
    d_lvl = int(d_lvl) if d_lvl else 1
    ofn = fn if fn else logfn
    if not msg or msg is None:
        return None

    # hide passwords
    msg = re.sub(r"(\w+)/(\w+)@(\w+)", r"\1/***@\3", msg)
    msg = re.sub(r"(password:)(\w+)", r"\1/***", msg, flags=re.IGNORECASE)

    if is_web:
        if re.search(r"^\s*\d+\.\s+\w+", msg):
            print(f1 % (fmt % (prg, step, msg)))
        if re.search(r"^ERR:", msg, re.IGNORECASE):
            print(f2 % ("red", fmt % (prg, step, msg)))
        if re.search(r"^WARN:", msg, re.IGNORECASE):
            print(f2 % ("orange", fmt % (prg, step, msg)))
        if re.search(r"^INFO:", msg, re.IGNORECASE):
            print(f2 % ("cyan", fmt % (prg, step, msg)))
        if re.search(r"^CMD:", msg, re.IGNORECASE):
            print(f2 % ("blue", fmt % (prg, step, msg)))
        if re.search(r"^\s*\d+\.\s+\w+:", msg):
            print("<br>")

    if lvl <= int(d_lvl) or lvl <= int(g_lvl):
        print(fmt % (prg, step, msg))
        if ofn and wrt2log >= 1:
            if not os.path.isfile(ofn):
                msg = "Logging to: " + ofn 
                print(fmt % (__name__, 5, msg) )
            with open(ofn, "a") as f:
                f.write(fmt % (prg, step, msg))
                f.write("\n")
    return None 


# Test cases
if __name__ == "__main__":
    # 1. Test with basic parameters
    echo_msg("TestProgram", 1.0, "Starting the program")
    # Expected output:
    # TestProgram: 1.0 - Starting the program


    # 2. Test with a message level lower than the global and debug levels
    os.environ["g_lvl"] = "2"
    os.environ["d_lvl"] = "2"
    echo_msg("TestProgram", 2.0, "INFO: Connecting to the server", 1)
    # Expected output: 
    # TestProgram: 2.0 - INFO: Connecting to the server

    # 3. Test with a message level higher than the global and debug levels:
    os.environ["g_lvl"] = "1"
    os.environ["d_lvl"] = "1"
    echo_msg("TestProgram", 3.0, "DEBUG: Verifying credentials", 3)
    # Expected output: No output, as the message level is higher than the 
    # global and debug levels.

    # 4. Test with password hiding:
    echo_msg("TestProgram", 4.0, "User/Password123@Database")
    # Expected output:
    # TestProgram: 4.0 - User/***@Database

    # 5. Test with log file output:
    os.environ["write2log"] = "1"
    os.environ["log_fn"] = "test_log.txt"
    echo_msg("TestProgram", 5.0, "INFO: Data processing completed", 1)
    # Expected output:
    # TestProgram: 5.0 - INFO: Data processing completed
    # And the message will also be written to the test_log.txt file.

    # 6. est with web environment (you would need to run this in a web 
    #    environment to see the formatted output):
    os.environ["QUERY_STRING"] = "test_query"
    os.environ["HTTP_HOST"] = "localhost"
    echo_msg("TestProgram", 6.0, "INFO: Web environment test", 1)
    # Expected output (in HTML formatting):
    # <font color="cyan">TestProgram: 6.0 - INFO: Web environment test</font>

# End of File
