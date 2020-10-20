import xml.dom.minidom
import sys
import os
import inspect
from datetime import datetime, date
from collections import OrderedDict
from optparse import OptionParser


PARSER_DATE_FORMAT = "%Y-%m-%d"
IMPORT_LINES = [
    "import os",
    "import sys",
    "import time",
    "from datetime import date",
    "from quant.universe import Universe",
    "from quant.constants import WORK_BASE_DIR, DATA_BASE_DIR, COMMON_FACTOR_FOR_TL, COMMON_FACTOR_FOR_MSCI",
    "from quant.universe_generator import UniverseGenerator",
    "from quant.data import ProjectData",
    "from quant.helpers import Logger"
]
REF_OBJECT_NAMES = set()
REF_OBJECT_NAMES.add("version_number")
REF_OBJECT_NAMES.add("public_dim_path_template")
ONE_INDENT = "    "
TWO_INDENT = ONE_INDENT + ONE_INDENT
PARAMETER_SPLITTER = ",\n" + TWO_INDENT
VERSION_NUMBER_LINES = [
    ONE_INDENT + "version_number = ProjectData.read_version_number(user_name, project_name)",
    ONE_INDENT + "ProjectData.save_version_number(user_name, project_name, version_number + 1)"
]
PUBLIC_DATA_LINES = [
    ONE_INDENT + "public_dim_path_template = {}"
]
ONE_EMPTY_LINE = ""
mode_setup = {
    "IS": {"required_data_source": ["Stock", "Index"],
           "start_date": date(2009, 1, 1),
           "end_date": date(2017, 6, 30),
           "back_days": 60,
           "end_days": 0
    },
    "OS": {"required_data_source": ["Stock", "Index"],
           "start_date": date(2017, 7, 1),
           "end_date": date(2019, 4, 11),
           "back_days": 250,
           "end_days": 0
    }
}


class ParsedObject(object):
    def __init__(self, name, arg_definitions, class_name=None):
        """
        :param name:
        :param arg_definitions: [(arg_name, arg_value), (), ...]
        """
        self.name = name
        self.arg_definitions = {arg_definition[0]: arg_definition for arg_definition in arg_definitions}
        self.class_name = class_name
        pass


class LiteralObject(object):
    def __init__(self, name, literal):
        self.name = name
        self.literal = literal


class RefObject(object):
    def __init__(self, ref_name):
        self.ref_name = ref_name

    pass


class ListRefObject(object):
    def __init__(self, ref_object_list):
        self.ref_object_list = ref_object_list


def parse_value(node_data, node_type, node_name):
    node_value = None
    if node_type == "ref_object":
        node_value = RefObject(node_data)
    elif node_type == "List[ref_object]":
        node_value = ListRefObject([RefObject(ref_name) for ref_name in node_data.strip().split(",")])
    elif node_type == "literal":
        node_value = LiteralObject(node_name, node_data)
    elif node_type == "str":
        node_value = node_data
    elif node_type == "bool":
        node_value = bool(node_data == "True")
    elif node_type == "int":
        node_value = int(node_data)
    elif node_type == "float":
        node_value = float(node_data)
    elif node_type == "date":
        node_value = datetime.strptime(node_data, PARSER_DATE_FORMAT).date()
    elif node_type == "List[str]":
        if node_data == "":
            return []
        node_value = node_data.strip().split(",")
    elif node_type == "List[bool]":
        if node_data == "":
            return []
        node_value = [bool(b) for b in node_data.strip().split(",")]
    elif node_type == "Map[str|str]":
        if node_data == "":
            return {}
        entries = node_data.strip().split(",")
        node_value = {}
        for e in entries:
            k, v = e.split(":")
            node_value[k] = v
    else:
        raise Exception("type {} not known for node {}!", node_type, node_name)

    if node_type != "ref_object":
        REF_OBJECT_NAMES.add(node_name)
    return node_value


def parse_node_from_module(current_module, node_name):
    try:
        node = get_first_child_node_by_name(current_module, node_name)
        node_type = node.getAttribute("type") if node.hasAttribute("type") else "ref_object"
        if node_type == "ref_object" and len(node.childNodes) == 0:
            return parse_value(None, node_type, node_name)
        if len(node.childNodes) == 0:
            return parse_value("", node_type, node_name)
        node_data = node.childNodes[0].data
        return parse_value(node_data, node_type, node_name)
    except:
        return None


def parse_args(args_module, candidate_args):
    parsed_args = {arg_name: parse_node_from_module(args_module, arg_name) for arg_name in candidate_args}
    for k, v in parsed_args.items():
        print(k, v)
    return parsed_args


def get_child_node_names(parent_module):
    return [n.tagName for n in parent_module.childNodes if type(n) == xml.dom.minidom.Element]


def get_child_nodes_by_name(parent_module, name):
    return [n for n in parent_module.childNodes if type(n) == xml.dom.minidom.Element and n.tagName == name]


def get_first_child_node_by_name(parent_module, name):
    return get_child_nodes_by_name(parent_module, name)[0]


def parse_config(config_module):
    args_module = get_first_child_node_by_name(config_module, "args")
    config_candidate_args = get_child_node_names(args_module)
    parsed_args = parse_args(args_module, config_candidate_args)
    config_object = ParsedObject("config", [(k, parsed_args[k]) for k in config_candidate_args])
    return config_object


def override_config(config_object, mode, user_name, project_name):
    if mode is None or mode not in mode_setup:
        return config_object
    config_object_arg_defs = config_object.arg_definitions
    tdict = {k: (k, v) for k,v in mode_setup[mode].items()}
    tdict = {**config_object_arg_defs, **tdict}
    if user_name is not None:
        user_name_dict = {"user_name": ("user_name", user_name)}
        tdict = {**tdict, **user_name_dict}
    if project_name is not None:
        user_name_dict = {"project_name": ("project_name", project_name + (("_" + mode) if mode is not None else ""))}
        tdict = {**tdict, **user_name_dict}
    config_object.arg_definitions = tdict
    return config_object


def parse_universe(universe_module):
    args_module = get_first_child_node_by_name(universe_module, "args")
    universe_candidate_args = get_child_node_names(args_module)
    parsed_args = parse_args(args_module, universe_candidate_args)
    universe_object = ParsedObject("universe", [(k, parsed_args[k]) for k in universe_candidate_args])
    REF_OBJECT_NAMES.add("universe")
    return universe_object


def parse_data_loaders(data_loaders_module):
    data_loader_modules = get_child_nodes_by_name(data_loaders_module, "data_loader")
    data_loaders = [parse_data_loader(m) for m in data_loader_modules]
    return data_loaders


def parse_operations(operations_module):
    operation_modules = get_child_nodes_by_name(operations_module, "operation")
    operations = [parse_operation(m) for m in operation_modules]
    return operations


def parse_alphas(alphas_module):
    alpha_modules = get_child_nodes_by_name(alphas_module, "alpha")
    alphas = [parse_alpha(m) for m in alpha_modules]
    return alphas

AUTHOR_ALPHA_PATH_TEMPLATE="/home/admin/submit_alpha/{}/alpha/"
AUTHOR_OP_PATH_TEMPLATE="/home/admin/submit_alpha/{}/operation/"

def parse_class_meta(xml_class_module, public_package_prefix):
    xml_class_name = xml_class_module.getAttribute("class")
    source = xml_class_module.getAttribute("source")
    author = xml_class_module.getAttribute("author")
    dirs_to_add = []
    if author is not None and author != "":
        dirs_to_add.append(os.path.dirname(AUTHOR_ALPHA_PATH_TEMPLATE.format(author)))
        dirs_to_add.append(os.path.dirname(AUTHOR_OP_PATH_TEMPLATE.format(author)))
    for d in dirs_to_add:
        sys.path.append(d)
    m = None
    if source == "public":
        module_name = xml_class_module.getAttribute("module")
        public_module_name = ".".join([public_package_prefix, module_name])
        m = __import__(public_module_name, fromlist=[""])
        IMPORT_LINES.append("from {} import {}".format(public_module_name, xml_class_name))
    elif source == "custom":
        module_path = xml_class_module.getAttribute("path")
        module_dir = os.path.dirname(module_path)
        module_name, ext = os.path.splitext(os.path.basename(module_path))
        sys.path.append(module_dir)
        m = __import__(module_name)
        sys.path.pop()
        IMPORT_LINES.append("sys.path.append(\"{}\")".format(module_dir))
        for d in dirs_to_add:
            IMPORT_LINES.append("sys.path.append(\"{}\")".format(d))
        IMPORT_LINES.append("from {} import {}".format(module_name, xml_class_name))
        IMPORT_LINES.append("sys.path.pop()")
        for d in dirs_to_add:
            IMPORT_LINES.append("sys.path.pop()")
    object_class = getattr(m, xml_class_name)
    init_parameters = [v for k, v in inspect.signature(object_class.__init__).parameters.items()]
    init_parameters_without_self = [p for p in init_parameters if p.name != "self"]
    for d in dirs_to_add:
        sys.path.pop()
    return xml_class_name, init_parameters_without_self


def get_final_args(parsed_args, init_parameters_without_self):
    final_args = OrderedDict()
    for p in init_parameters_without_self:
        if p.name in parsed_args:
            final_args[p.name] = parsed_args[p.name]
        elif p.default != inspect._empty:
            final_args[p.name] = p.default
        else:
            if p.name in REF_OBJECT_NAMES:
                final_args[p.name] = RefObject(p.name)
    return final_args


def parse_data_loader(data_loader_module):
    data_loader_class_name, init_parameters_without_self = parse_class_meta(data_loader_module, "quant.public_dl")

    args_module = get_first_child_node_by_name(data_loader_module, "args")
    data_loader_candidate_args = get_child_node_names(args_module)
    parsed_args = parse_args(args_module, data_loader_candidate_args)
    if "data_loader_name" not in data_loader_candidate_args:
        raise Exception("data_loader_name not defined for data_loader in definitions {}", args_module)
    final_args = get_final_args(parsed_args, init_parameters_without_self)
    REF_OBJECT_NAMES.add(parsed_args["data_loader_name"])
    return ParsedObject(parsed_args["data_loader_name"], [(k, v) for k, v in final_args.items()],
                        data_loader_class_name)


def parse_operation(operation_module):
    operation_class_name, init_parameters_without_self = parse_class_meta(operation_module, "quant.public_op")
    args_module = get_first_child_node_by_name(operation_module, "args")
    operation_name = parse_node_from_module(operation_module, "name")
    operation_candidate_args = get_child_node_names(args_module)
    if operation_name is None:
        raise Exception("name not defined for operation in definitions {}", operation_module)
    parsed_args = parse_args(args_module, operation_candidate_args)
    final_args = get_final_args(parsed_args, init_parameters_without_self)
    REF_OBJECT_NAMES.add(operation_name)
    return ParsedObject(operation_name, [(k, v) for k, v in final_args.items()], operation_class_name)


def parse_alpha(alpha_module):
    alpha_class_name, init_parameters_without_self = parse_class_meta(alpha_module, "quant.public_alpha")

    args_module = get_first_child_node_by_name(alpha_module, "args")
    alpha_candidate_args = get_child_node_names(args_module)
    if "alpha_name" not in alpha_candidate_args:
        raise Exception("alpha_name not defined for data_loader in definitions {}", args_module)
    parsed_args = parse_args(args_module, alpha_candidate_args)
    final_args = get_final_args(parsed_args, init_parameters_without_self)
    REF_OBJECT_NAMES.add(parsed_args["alpha_name"])
    return ParsedObject(parsed_args["alpha_name"], [(k, v) for k, v in final_args.items()], alpha_class_name)


def parse_trade_and_stats_engine(trade_and_stats_engine_module):
    trade_and_stats_engine_class_name, init_parameters_without_self = parse_class_meta(trade_and_stats_engine_module,
                                                                                       "quant")
    args_module = get_first_child_node_by_name(trade_and_stats_engine_module, "args")
    trade_and_stats_engine_name = parse_node_from_module(trade_and_stats_engine_module, "name")
    trade_and_stats_engine_candidate_args = get_child_node_names(args_module)
    if trade_and_stats_engine_name is None:
        raise Exception("name not defined for trade_and_stats_engine in definitions {}", trade_and_stats_engine_module)
    parsed_args = parse_args(args_module, trade_and_stats_engine_candidate_args)
    final_args = get_final_args(parsed_args, init_parameters_without_self)
    REF_OBJECT_NAMES.add(trade_and_stats_engine_name)
    return ParsedObject(trade_and_stats_engine_name, [(k, v) for k, v in final_args.items()],
                        trade_and_stats_engine_class_name)


def parse_back_test_engine(back_test_engine_module):
    back_test_engine_class_name, init_parameters_without_self = parse_class_meta(back_test_engine_module,
                                                                                 "quant")
    args_module = get_first_child_node_by_name(back_test_engine_module, "args")
    back_test_engine_name = parse_node_from_module(back_test_engine_module, "name")
    back_test_engine_candidate_args = get_child_node_names(args_module)
    if back_test_engine_name is None:
        raise Exception("name not defined for back_test_engine in definitions {}", back_test_engine_module)
    parsed_args = parse_args(args_module, back_test_engine_candidate_args)
    final_args = get_final_args(parsed_args, init_parameters_without_self)
    REF_OBJECT_NAMES.add(back_test_engine_name)
    return ParsedObject(back_test_engine_name, [(k, v) for k, v in final_args.items()], back_test_engine_class_name)


def resolve_arg_definition(arg_definition):
    """
    :param arg_definition: (arg_name, arg_value)
    :return: one piece of arg: arg_name=arg_value
    """
    arg_name = arg_definition[0]
    arg_value = arg_definition[1]
    if type(arg_value) == RefObject:
        return "{}={}".format(arg_name, arg_value.ref_name)
    elif type(arg_value) == ListRefObject:
        return "{}=[{}]".format(arg_name, ", ".join([ro.ref_name for ro in arg_value.ref_object_list]))
    elif type(arg_value) == str:
        return "{}=\"{}\"".format(arg_name, arg_value)
    elif type(arg_value) == LiteralObject:
        return "{}={}".format(arg_name, arg_value.literal)
    elif type(arg_value) == date:
        return "{}=date({}, {}, {})".format(arg_name, arg_value.year, arg_value.month, arg_value.day)
    else:
        return "{}={}".format(arg_name, arg_value)


def resolve_config(config_object):
    return [ONE_INDENT + resolve_arg_definition(arg_definition) for k, arg_definition in
            config_object.arg_definitions.items()]


def resolve_universe(universe_object):
    arg_list = [resolve_arg_definition(arg_definition) for k, arg_definition in universe_object.arg_definitions.items()]
    universe_code_lines = [
        ONE_INDENT + ("universe_generator = UniverseGenerator(\n" + TWO_INDENT + "{}\n" + ONE_INDENT + ")").format(
            PARAMETER_SPLITTER.join(arg_list))
    ]
    universe_code_lines.extend([
        ONE_INDENT + "universe_path = os.path.join(WORK_BASE_DIR, \"{}/{}/universe/universe.bin\".format(user_name, project_name))",
        ONE_INDENT + "universe = Universe.new_universe_from_file(universe_path)"
    ])
    return universe_code_lines


def resolve_data_loader(data_loader_object):
    arg_list = [resolve_arg_definition(arg_definition) for k, arg_definition in
                data_loader_object.arg_definitions.items()]
    lines = []
    lines.extend([
        ONE_INDENT + ("{} = {}(\n" + TWO_INDENT + "{}\n" + ONE_INDENT + ")").format(
            data_loader_object.name, data_loader_object.class_name, PARAMETER_SPLITTER.join(arg_list)),
        ONE_INDENT + "{}.do_load()".format(data_loader_object.name),
    ])
    return lines


def resolve_operation(operation_object):
    arg_list = [resolve_arg_definition(arg_definition) for k, arg_definition in
                operation_object.arg_definitions.items()]
    lines = [
        ONE_INDENT + ("{} = {}(\n" + TWO_INDENT + "{}\n" + ONE_INDENT + ")").format(
            operation_object.name, operation_object.class_name, PARAMETER_SPLITTER.join(arg_list))]
    return lines


def resolve_alpha(alpha_object):
    arg_list = [resolve_arg_definition(arg_definition) for k, arg_definition in alpha_object.arg_definitions.items()]
    lines = [
        ONE_INDENT + ("{} = {}(\n" + TWO_INDENT + "{}\n" + ONE_INDENT + ")").format(
            alpha_object.name, alpha_object.class_name, PARAMETER_SPLITTER.join(arg_list))]
    return lines


def resolve_trade_and_stats_engine(trade_and_stats_engine_object):
    arg_list = [resolve_arg_definition(arg_definition) for k, arg_definition in
                trade_and_stats_engine_object.arg_definitions.items()]
    lines = [
        ONE_INDENT + ("{} = {}(\n" + TWO_INDENT + "{}\n" + ONE_INDENT + ")").format(
            trade_and_stats_engine_object.name, trade_and_stats_engine_object.class_name,
            PARAMETER_SPLITTER.join(arg_list))]
    return lines


def resolve_back_test_engine(back_test_engine_object):
    arg_list = [resolve_arg_definition(arg_definition) for k, arg_definition in
                back_test_engine_object.arg_definitions.items()]
    lines = []
    lines.extend([
        ONE_INDENT + ("{} = {}(\n" + TWO_INDENT + "{}\n" + ONE_INDENT + ")").format(
            back_test_engine_object.name, back_test_engine_object.class_name, PARAMETER_SPLITTER.join(arg_list)),
        ONE_INDENT + "{}.run_test()".format(back_test_engine_object.name),
    ])
    return lines


def parse_project(xml_file_path, generated_python_file_path, mode=None, user_name=None, project_name=None):
    dom_tree = xml.dom.minidom.parse(xml_file_path)
    project = dom_tree.documentElement
    code_lines = [
        "\n", "if __name__ == \"__main__\":",
        ONE_INDENT + "t0 = time.process_time()"
    ]

    print("----Config----")
    config_modules = get_child_nodes_by_name(project, "config")
    if len(config_modules) > 1:
        raise Exception("Multiple config module found! Expected only one!")
    parsed_config_object = parse_config(config_modules[0])
    parsed_config_object = override_config(parsed_config_object, mode, user_name, project_name)
    code_lines.extend(resolve_config(parsed_config_object))
    code_lines.append(ONE_EMPTY_LINE)

    print("----Universe----")
    universe_modules = get_child_nodes_by_name(project, "universe")
    if len(universe_modules) > 1:
        raise Exception("Multiple universe module found! Expected only one!")
    parsed_universe_object = parse_universe(universe_modules[0])
    code_lines.extend(resolve_universe(parsed_universe_object))
    code_lines.append(ONE_EMPTY_LINE)

    code_lines.extend(VERSION_NUMBER_LINES)
    code_lines.append(ONE_EMPTY_LINE)

    print("----Data Loaders----")
    code_lines.extend(PUBLIC_DATA_LINES)
    data_loaders_modules = get_child_nodes_by_name(project, "data_loaders")
    if len(data_loaders_modules) > 1:
        raise Exception("Multiple data_loaders module found! Expected only one!")
    parsed_data_loader_objects = parse_data_loaders(data_loaders_modules[0])
    for o in parsed_data_loader_objects:
        code_lines.extend(resolve_data_loader(o))
        code_lines.append(ONE_EMPTY_LINE)





    print("----Operations----")
    operations_modules = get_child_nodes_by_name(project, "operations")
    if len(operations_modules) > 1:
        raise Exception("Multiple operations module found! Expected only one!")
    parsed_operation_objects = parse_operations(operations_modules[0])
    for o in parsed_operation_objects:
        code_lines.extend(resolve_operation(o))
    code_lines.append(ONE_EMPTY_LINE)

    print("----Alphas----")
    alphas_modules = get_child_nodes_by_name(project, "alphas")
    if len(alphas_modules) > 1:
        raise Exception("Multiple alphas module found! Expected only one!")
    parsed_alpha_objects = parse_alphas(alphas_modules[0])
    for o in parsed_alpha_objects:
        code_lines.extend(resolve_alpha(o))
    code_lines.append(ONE_EMPTY_LINE)

    print("----Trade and Stats----")
    trade_and_stats_engine_modules = get_child_nodes_by_name(project, "trade_and_stats_engine")
    if len(trade_and_stats_engine_modules) > 1:
        raise Exception("Multiple trade_and_stats_engine module found! Expected only one!")
    parsed_trade_and_stats_engine_object = parse_trade_and_stats_engine(trade_and_stats_engine_modules[0])
    code_lines.extend(resolve_trade_and_stats_engine(parsed_trade_and_stats_engine_object))
    code_lines.append(ONE_EMPTY_LINE)

    print("----Back Test----")
    back_test_engine_modules = get_child_nodes_by_name(project, "back_test_engine")
    if len(back_test_engine_modules) > 1:
        raise Exception("Multiple back_test_engine module found! Expected only one!")
    parsed_back_test_engine_object = parse_back_test_engine(back_test_engine_modules[0])
    code_lines.extend(resolve_back_test_engine(parsed_back_test_engine_object))
    code_lines.append(ONE_EMPTY_LINE)

    code_lines.extend([
        ONE_INDENT + "t1 = time.process_time()",
        ONE_INDENT + "Logger.info(\"\", \"{} seconds\".format(t1 - t0))"
    ])

    print("----OUTPUT CODE START----")
    for line in IMPORT_LINES:
        print(line)
    for line in code_lines:
        print(line)
    print("----OUTPUT CODE END----")
    with open(generated_python_file_path, "w") as f:
        for line in IMPORT_LINES:
            f.write(line + "\n")
        for line in code_lines:
            f.write(line + "\n")


if __name__ == "__main__":
    usage = "Usage: %prog -i input_xml_path -o output_python_file_path"
    parser = OptionParser(usage)
    parser.add_option("-i", "--input", action="store", type="string", dest="input_path", help="input xml")
    parser.add_option("-o", "--output", action="store", type="string", dest="output_path", help="output python file")
    parser.add_option("-m", "--mode", action="store", type="string", dest="mode", help="IS, OS, Funda_IS, Funda_OS, Flow_IS")
    parser.add_option("-u", "--user", action="store", type="string", dest="user_name", help="user_name")
    parser.add_option("-p", "--project", action="store", type="string", dest="project_name", help="project_name")
    (options, args) = parser.parse_args()
    if options.input_path is None:
        parser.error("missing -i to identify input xml path")
    if options.output_path is None:
        parser.error("missing -o to identify output python file path")
    parse_project(options.input_path, options.output_path, mode=options.mode, user_name=options.user_name, project_name=options.project_name)
    #parse_project("/home/admin/xml_test/test.xml", "/home/admin/xml_test/generated_all_in_one.py")
