"""
Sphinx extension to automatically generate "Used By" sections for classes and functions.

This extension analyzes the Python source code to find where each class/function
is imported and used, then adds this information to the documentation.
"""

import ast
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

from docutils import nodes
from sphinx import addnodes
from sphinx.application import Sphinx


class UsageAnalyzer(ast.NodeVisitor):
    """AST visitor that collects usage information from Python source files."""

    def __init__(self, module_name: str):
        self.module_name = module_name
        self.imports: Dict[str, str] = {}  # local_name -> full_module_path
        self.usages: List[Tuple[str, str, int]] = []  # (name, context, line_number)
        self.current_class = None
        self.current_function = None

    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            name = alias.asname if alias.asname else alias.name
            self.imports[name] = alias.name
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        module = node.module or ''
        for alias in node.names:
            name = alias.asname if alias.asname else alias.name
            if module:
                full_path = f"{module}.{alias.name}"
            else:
                full_path = alias.name
            self.imports[name] = full_path
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef):
        old_class = self.current_class
        self.current_class = node.name
        self.generic_visit(node)
        self.current_class = old_class

    def visit_FunctionDef(self, node: ast.FunctionDef):
        old_function = self.current_function
        self.current_function = node.name
        self.generic_visit(node)
        self.current_function = old_function

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Call(self, node: ast.Call):
        # Get the name being called
        name = self._get_call_name(node.func)
        if name:
            context = self._get_context()
            self.usages.append((name, context, node.lineno))
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute):
        # Track attribute access like `module.Class` or `obj.method`
        name = self._get_attribute_chain(node)
        if name:
            context = self._get_context()
            self.usages.append((name, context, node.lineno))
        self.generic_visit(node)

    def _get_call_name(self, node) -> str:
        """Extract the name from a call node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return self._get_attribute_chain(node)
        return None

    def _get_attribute_chain(self, node: ast.Attribute) -> str:
        """Get the full attribute chain like 'module.Class.method'."""
        parts = []
        current = node
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
        parts.reverse()
        return '.'.join(parts) if parts else None

    def _get_context(self) -> str:
        """Get the current context (class.method or function name)."""
        if self.current_class and self.current_function:
            return f"{self.current_class}.{self.current_function}"
        elif self.current_function:
            return self.current_function
        elif self.current_class:
            return self.current_class
        return "<module>"


def analyze_source_directory(src_dir: str) -> Dict[str, List[dict]]:
    """
    Analyze all Python files in the source directory and build a usage map.

    Returns a dict mapping fully qualified names to lists of usage information.
    """
    usage_map = defaultdict(list)
    src_path = Path(src_dir)

    if not src_path.exists():
        return usage_map

    # Find all Python files
    for py_file in src_path.rglob('*.py'):
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                source = f.read()

            tree = ast.parse(source, filename=str(py_file))

            # Determine module name from file path
            rel_path = py_file.relative_to(src_path)
            if rel_path.name == '__init__.py':
                module_name = '.'.join(rel_path.parent.parts)
            else:
                module_name = '.'.join(rel_path.with_suffix('').parts)

            if not module_name:
                module_name = py_file.stem

            analyzer = UsageAnalyzer(module_name)
            analyzer.visit(tree)

            # Map usages to their full module paths
            for name, context, line in analyzer.usages:
                # Try to resolve the name through imports
                parts = name.split('.')
                base = parts[0]

                if base in analyzer.imports:
                    # Resolve through import
                    imported_module = analyzer.imports[base]
                    if len(parts) > 1:
                        full_name = f"{imported_module}.{'.'.join(parts[1:])}"
                    else:
                        full_name = imported_module
                else:
                    full_name = name

                # Record the usage
                usage_info = {
                    'used_in_module': module_name,
                    'used_in_context': context,
                    'line': line,
                    'file': str(py_file),
                }
                usage_map[full_name].append(usage_info)

        except (SyntaxError, UnicodeDecodeError) as e:
            # Skip files that can't be parsed
            continue

    return usage_map


def build_usage_map(app: Sphinx):
    """Build the usage map and store it in the Sphinx environment."""
    # Get the source directory from config
    src_dir = app.config.usedby_source_dir
    if not src_dir:
        return

    # Make path absolute if relative
    if not os.path.isabs(src_dir):
        src_dir = os.path.join(app.srcdir, src_dir)

    app.env.usedby_map = analyze_source_directory(src_dir)
    # Store pending usedby data for objects (to be added during doctree transform)
    app.env.usedby_pending = {}


def collect_usedby_for_object(app: Sphinx, what: str, name: str, obj, options, lines: List[str]):
    """
    Autodoc event handler that collects 'Used By' information for later insertion.
    """
    # Skip properties and attributes - they don't need "Used By" sections
    if what in ('attribute', 'property'):
        return

    if not hasattr(app.env, 'usedby_map'):
        return

    usage_map = app.env.usedby_map

    # Try different name variations to find usages
    name_variations = [name]

    # Also try the short name (last part)
    short_name = name.split('.')[-1]
    if short_name != name:
        name_variations.append(short_name)

    # Collect all usages
    all_usages = []
    seen_contexts = set()

    for name_var in name_variations:
        for key, usages in usage_map.items():
            # Check if this key refers to our object
            if key == name_var or key.endswith(f'.{name_var}') or name_var.endswith(f'.{key}'):
                for usage in usages:
                    # Skip self-references (same module)
                    if usage['used_in_module'] == name.rsplit('.', 1)[0]:
                        continue

                    context_key = (usage['used_in_module'], usage['used_in_context'])
                    if context_key not in seen_contexts:
                        seen_contexts.add(context_key)
                        all_usages.append(usage)

    if not all_usages:
        return

    # Store the usages for this object to be processed during doctree transform
    app.env.usedby_pending[name] = all_usages


def compute_relative_url(current_docname: str, target_doc_path: str, anchor: str) -> str:
    """
    Compute a relative URL from the current document to the target document.

    Args:
        current_docname: The current document name (e.g., 'api/apps/create_job_task_list')
        target_doc_path: The target document path (e.g., 'apps/create_job_task_list')
        anchor: The anchor fragment (e.g., 'module-ecco_dataset_production.apps')

    Returns:
        A proper relative URL from current to target
    """
    # Get the directory of the current document
    current_dir = os.path.dirname(current_docname)

    # The target_doc_path is relative to 'api/' directory
    # Construct the full target docname
    target_docname = f"api/{target_doc_path}"

    # Compute relative path from current directory to target
    if current_dir:
        # Count how many levels deep we are
        current_parts = current_dir.split('/')
        target_dir = os.path.dirname(target_docname)
        target_parts = target_dir.split('/') if target_dir else []

        # Find common prefix
        common_length = 0
        for i in range(min(len(current_parts), len(target_parts))):
            if current_parts[i] == target_parts[i]:
                common_length = i + 1
            else:
                break

        # Go up from current to common ancestor
        up_count = len(current_parts) - common_length
        up_path = '../' * up_count

        # Go down from common ancestor to target
        down_parts = target_parts[common_length:]
        down_path = '/'.join(down_parts)

        if down_path:
            relative_path = f"{up_path}{down_path}/{os.path.basename(target_docname)}.html"
        else:
            relative_path = f"{up_path}{os.path.basename(target_docname)}.html"
    else:
        relative_path = f"{target_docname}.html"

    return f"{relative_path}#{anchor}"


def create_usedby_field(app: Sphinx, all_usages: List[dict], current_docname: str) -> nodes.field:
    """Create a field node for the 'Used By' section."""
    package_prefix = app.config.usedby_package_name

    # Group usages by module
    by_module = defaultdict(list)
    for usage in all_usages:
        by_module[usage['used_in_module']].append(usage)

    # Create the field node
    field = nodes.field()

    # Field name
    field_name = nodes.field_name('', 'Used By')
    field += field_name

    # Field body with bullet list
    field_body = nodes.field_body()
    bullet_list = nodes.bullet_list()

    for module, usages in sorted(by_module.items()):
        contexts = sorted(set(u['used_in_context'] for u in usages))
        full_module = f"{package_prefix}.{module}" if package_prefix else module

        for context in contexts:
            # Target document path (relative to api/ directory)
            doc_path = module.replace('.', '/')

            if context == "<module>":
                display_name = full_module
                anchor = f"module-{full_module}"
            elif '.' in context:
                class_name, method_name = context.rsplit('.', 1)
                full_target = f"{full_module}.{class_name}.{method_name}"
                display_name = f"{full_module}.{class_name}.{method_name}()"
                anchor = full_target
            else:
                full_target = f"{full_module}.{context}"
                if context[0].isupper():
                    display_name = f"{full_module}.{context}"
                else:
                    display_name = f"{full_module}.{context}()"
                anchor = full_target

            # Compute proper relative URL
            url = compute_relative_url(current_docname, doc_path, anchor)

            # Create list item with reference
            list_item = nodes.list_item()
            para = nodes.paragraph()
            ref = nodes.reference('', display_name, refuri=url)
            para += ref
            list_item += para
            bullet_list += list_item

    field_body += bullet_list
    field += field_body

    return field


def process_desc_content(app: Sphinx, domain: str, objtype: str, contentnode: addnodes.desc_content):
    """
    Process object description content to add 'Used By' field to existing field list.
    """
    if not hasattr(app.env, 'usedby_pending'):
        return

    # Skip attributes and properties
    if objtype in ('attribute', 'property'):
        return

    # Find the object name from the parent desc node
    parent = contentnode.parent
    if not isinstance(parent, addnodes.desc):
        return

    # Get the signature to find the object name
    for sig in parent.traverse(addnodes.desc_signature):
        # Try to get the full name from ids
        if sig.get('ids'):
            obj_name = sig['ids'][0]
            break
        # Fallback to module + names
        module = sig.get('module', '')
        names = sig.get('fullname', '')
        if module and names:
            obj_name = f"{module}.{names}"
            break
    else:
        return

    # Check if we have usedby data for this object
    if obj_name not in app.env.usedby_pending:
        return

    all_usages = app.env.usedby_pending[obj_name]

    # Get the current document name for computing relative URLs
    current_docname = app.env.docname

    # Create the Used By field
    usedby_field = create_usedby_field(app, all_usages, current_docname)

    # Find existing field list or create one
    field_list = None
    for node in contentnode.children:
        if isinstance(node, nodes.field_list):
            field_list = node
            break

    if field_list is None:
        # Create a new field list and insert it at the beginning after any paragraph
        field_list = nodes.field_list()
        field_list['classes'].append('simple')

        # Find insertion point (after first paragraph if exists)
        insert_idx = 0
        for i, child in enumerate(contentnode.children):
            if isinstance(child, nodes.paragraph):
                insert_idx = i + 1
                break

        contentnode.insert(insert_idx, field_list)

    # Insert the Used By field at the beginning of the field list
    field_list.insert(0, usedby_field)


def setup(app: Sphinx):
    """Set up the Sphinx extension."""
    # Configuration values
    app.add_config_value('usedby_source_dir', None, 'env')
    app.add_config_value('usedby_package_name', '', 'env')

    # Connect to events
    app.connect('builder-inited', build_usage_map)
    app.connect('autodoc-process-docstring', collect_usedby_for_object)
    app.connect('object-description-transform', process_desc_content)

    return {
        'version': '1.0',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
