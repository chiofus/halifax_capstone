def add_venv_dir_to_modules(path_to_check: str = '') -> None:
    import os, sys
    from pathlib import Path

    if not path_to_check: path_to_check = os.path.dirname(os.path.abspath(__file__))
    p = Path(path_to_check)

    dirs = [item.name for item in p.iterdir() if item.is_dir()]
    if '.venv' in dirs:
        sys.path.append(path_to_check)
        return None
    
    add_venv_dir_to_modules(p.parent.as_posix()) 

#Main agent program
if __name__ == "__main__":
    #Add .venv dir to modules
    add_venv_dir_to_modules()

    #Imports
    from agent_logic import initialize_agent, process_query_response
    from objects.objects import ALL_QUERIES_REF

    initialize_agent(model_name="gpt-5.4")
    # process_query_response(ALL_QUERIES_REF[0], [], 'model_name', 'client')

    exit()