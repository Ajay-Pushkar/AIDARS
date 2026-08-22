import re
from pathlib import Path

def patch_exporter():
    file_path = Path(r"C:\Users\kurap\.gemini\antigravity\scratch\extracted_files\AIDAR_project\AIDAR\src\aidars\scene_intelligence\exporters.py")
    content = file_path.read_text(encoding="utf-8")
    
    new_method = '''    @staticmethod
    def to_dict(graph: "DependencyGraph") -> dict[str, Any]:
        """Convert a dependency graph into a JSON-serializable dictionary."""
        from .integrity import IntegrityChecker

        integrity = IntegrityChecker().check(graph)
        
        dependencies = {}
        has_incoming = set()
        
        for node in graph.nodes:
            dependencies[node.identifier] = {
                "type": node.kind.capitalize(),
                "label": node.label,
                "children": []
            }
            
        for edge in graph.edges:
            if edge.source in dependencies:
                if edge.target not in dependencies[edge.source]["children"]:
                    dependencies[edge.source]["children"].append(edge.target)
            has_incoming.add(edge.target)
            
        scene_children = [
            node.identifier for node in graph.nodes if node.identifier not in has_incoming
        ]
        
        dependencies["Scene"] = {
            "type": "Root",
            "label": "Scene",
            "children": scene_children
        }

        return {
            "schema_version": "1.0",
            "project_root": "Scene",
            "dependencies": dependencies,
            "statistics": {
                "node_count": len(graph.nodes),
                "edge_count": len(graph.edges),
            },
            "integrity": integrity.to_dict(),
        }'''
    
    pattern = re.compile(r'    @staticmethod\n    def to_dict\(graph: "DependencyGraph"\) -> dict\[str, Any\]:.*?return {.*?}', re.DOTALL)
    
    new_content, count = pattern.subn(new_method, content, count=1)
    if count == 1:
        file_path.write_text(new_content, encoding="utf-8")
        print("Successfully patched DependencyGraphExporter in exporters.py")
    else:
        print("Failed to find pattern in exporters.py")

if __name__ == "__main__":
    patch_exporter()
