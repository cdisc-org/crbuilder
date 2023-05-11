# Purpose: A program to print to console or write to a file a formatted message
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/15/2023 (htu) - initial coding based on echo_msg from R
#   03/16/2023 (htu) - added test cases
#   03/17/2023 (htu) - removed "\n" from fmt
#   03/30/2023 (htu) - added logic to write to a file 
#   04/07/2023 (htu) - 
#     1. added YAML, and comments objects to write these objects
#     2. added g_msg_lvl and g_log_lvl
#     3. added i and n and level 1 and 2 logging 
#   04/08/2023 (htu) - writing v_msg to all the log files 
#   05/05/2023 (htu) - added msg_type and msg_txt to print object type
#   05/06/2023 (htu) - added more cases for detecting message type and logging
# 
import os
import re
import pandas as pd 
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

def echo_msg(prg, step, msg, lvl=0, fn=None, i:int=0, n:int=0):
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

    def dump_yml(prg, step, msg, fmt, fh, i:int=1, n:int=1):
        msg_txt = fmt % (prg, step, f"{i}/{n}:\n")
        y = YAML()
        y.indent(mapping=2, sequence=4, offset=2)
        y.preserve_quotes = True
        fh.write(msg_txt)
        y.dump(msg, fh)
        # fh.write("\n\n")
    # End of dump_yml

    fmt = "%s: %.3f - %s"
    f1 = "<h2>%s</h2>"
    f2 = '<font color="%s">%s</font>'
    g_msg_lvl = os.getenv("g_msg_lvl")          # Global message level
    g_log_lvl = os.getenv("g_log_lvl")          # Global log/debug level
    if g_msg_lvl is None: 
        g_msg_lvl = os.getenv("g_lvl")          # message level
    if g_log_lvl is None: 
        g_log_lvl = os.getenv("d_lvl")          # debug/log level
    logfn = os.getenv("log_fn")         # detailed log file name
    log_f1 = os.getenv("log_fn_1")      # 1st level message log file name 
    log_f2 = os.getenv("log_fn_2")      # 2nd level message log file name 
    w2log = os.getenv("write2log")  # whether to write to log file: 1 - Yes, 0 - No 
    wrt2log = 0 if w2log is None else int(w2log)
    query_str = os.getenv("QUERY_STRING")
    http_host = os.getenv("HTTP_HOST")
    is_web = bool(query_str and http_host)

    g_msg_lvl = int(g_msg_lvl) if g_msg_lvl else 1
    g_log_lvl = int(g_log_lvl) if g_log_lvl else 1
    ofn = fn if fn else logfn
    b1 = True if (isinstance(msg, pd.DataFrame) and msg.empty) else False
    b2 = msg is None
    # b3 = not msg 
    if b1 or b2:
        msg_type = type(msg) 
        m0 = fmt % (prg, step, f"Empty MSG:")
        m1 = f"from (Object Type - {msg_type})"
        m2 = "-" * len(m1)
        msg_txt = f"{m0}\n{m1}\n{m2}\n\n" 
        if ofn and wrt2log >= 1 and int(g_log_lvl) > 5:
             with open(ofn, "a") as f:
                f.write(msg_txt)
        else:
            print(msg_txt)
        return None

    # hide passwords
    if isinstance(msg, str): 
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

    if lvl <= int(g_msg_lvl):
        if i > 0:
            print(fmt % (prg, step, f"({i}/{n}): {msg}"))
        else: 
            print(fmt % (prg, step, msg))
    if log_f1 and wrt2log >= 1:
        if not os.path.isfile(log_f1) and lvl == 1:
            print(f"*L1 Logging to: {log_f1}" )
        if lvl <= 1:
            with open(log_f1, "a") as f:
                f.write(fmt % (prg, step, f"{msg}\n"))
    if log_f2 and wrt2log >= 1:
        if not os.path.isfile(log_f2) and lvl == 2:
            print(f"*L2 Logging to: {log_f2}" )
        if lvl <= 2:
            with open(log_f2, "a") as f:
                f.write(fmt % (prg, step, f"{msg}\n"))

    if lvl <= int(g_log_lvl):
        if ofn and wrt2log >= 1:
            msg_type = type(msg) 
            m1 = f"(Object Type: {msg_type})"
            m2 = "-" * len(m1)
            msg_txt = f"{m1}\n{m2}\n" 

            if not os.path.isfile(ofn): 
                v_msg = f"  {i}/{n} - Logging to: {ofn}"
                print(v_msg)
                if log_f1 is not None:
                    with open(log_f1, "a") as f:
                        f.write(f"{v_msg}\n")
                if log_f2 is not None: 
                    with open(log_f2, "a") as f:
                        f.write(f"{v_msg}\n")
                if ofn is not None: 
                    with open(ofn, "a") as f:
                        f.write(f"{v_msg}\n")
            with open(ofn, "a") as f:
                if int(g_log_lvl) > 5:
                    f.write(msg_txt)
                if isinstance(msg, (dict, CommentedMap, CommentedSeq)):
                    dump_yml(prg, step, msg, fmt, fh=f)
                elif isinstance(msg, pd.DataFrame):
                    m0 = fmt % (prg, step, f"pd.DataFrame:\n")
                    f.write(m0)
                    f.write(msg.to_string())
                    f.write("\n")
                elif isinstance(msg, (list)):
                    msg_n = len(msg)
                    for i in range(msg_n):
                        lst = msg[i] 
                        if isinstance(lst, (dict, CommentedMap, CommentedSeq)):
                            dump_yml(prg, step, lst, fmt, fh=f, i=i+1, n=msg_n)
                        elif isinstance(lst, pd.DataFrame):
                            m0 = fmt % (prg, step, f"pd.DataFrame ({i}/{msg_n}):\n")
                            f.write(m0)
                            f.write(lst.to_string())
                            f.write("\n")
                        else: 
                            f.write(fmt % (prg, step, f"{msg}\n"))
                else: 
                    if i > 0:
                        f.write(fmt % (prg, step, f"({i}/{n}) {msg}\n"))
                    else:
                        f.write(fmt % (prg, step, f"{msg}\n"))
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
