import argparse
import textwrap

from runners import DefaultRunner
from settings import clear
from validation import validate_positive_int, validate_str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rotina",
        description=textwrap.dedent("""
            Manipulação de rotinas utilizando o terminal.
        """),
        epilog=textwrap.dedent("""
            
        """),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    ### PARSER CREATE
    create_parser = subparsers.add_parser(
        "cadastro",
        # aliases=["new", "add"],
        description=textwrap.dedent("""
                Use esse comando para
                Use this command to create a new task quickly and efficiently.

                Provide a title, optional tags, priority, and mark it as done if needed.
                Whether you're planning your day or dumping ideas into the terminal,
                this is your entry point.
            """),
        epilog=textwrap.dedent("""
                Examples:

                  task create -t "Buy groceries"
                  task create -t "Study argparse" --tag python --tag cli --priority high
                  task create -t "Walk the dog" --done

                You can also combine options freely to match your workflow.
                Tags help with filtering later. Priorities can be: low, medium, high.
            """),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # create_parser.set_defaults(command="create")




    return parser


def run() -> None:
    clear()
    parser = build_parser()

    args = parser.parse_args()

    default_runner = DefaultRunner()
    getattr(default_runner, args.command)(args)


if __name__ == "__main__":
    run()