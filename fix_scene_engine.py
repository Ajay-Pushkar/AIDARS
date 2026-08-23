import re
from pathlib import Path

f = Path("src/aidars/scene_intelligence/scene_engine.py")
content = f.read_text("utf-8")

# 1. Update imports
content = re.sub(
    r"from aidars.smart_package.builder import \(.*?\)",
    "from aidars.smart_package.builder import (\n    PackageBuilder,\n    PackagePlanner,\n)\nfrom aidars.scheduler.optimizer import PackageAsset",
    content,
    flags=re.DOTALL
)

# 2. Update SceneEngineResult
content = re.sub(
    r"package: Optional\[PackageManifest\] = None\n\s+",
    "",
    content
)

# 3. Update run()
run_replacement = """        if request.build_package:
            graph_for_packaging = result.graph or self.build_dependency_graph(result.snapshot)
            if request.optimize_package_by_visibility:
                render_req = RenderRequest(
                    camera_id=request.camera_id,
                    frame_start=request.frame_start,
                    frame_end=request.frame_end,
                )
                req_report = self.analyze_render_requirements(
                    snapshot=result.snapshot,
                    graph=graph_for_packaging,
                    request=render_req,
                )
                result.render_requirements = req_report
                result.visibility = self.analyze_visibility(result.snapshot, request.frame_start, request.frame_end)
    
                required_object_ids = set(req_report.required_objects)
                result.messages.append(
                    f"Render requirement analysis: {len(required_object_ids)} object(s) required "
                    f"in frames {request.frame_start}-{request.frame_end}"
                )
    
                seed_ids = RequirementResolver.resolve(req_report)
            else:
                seed_ids = {node.node_id for node in graph_for_packaging.nodes}

            closure_ids = DependencyClosureResolver.compute_closure(seed_ids, graph_for_packaging)
            
            input_p = Path(request.input_path) if request.input_path else Path.cwd()
            base_dir = input_p.parent if input_p.exists() and input_p.is_file() else Path.cwd()
            
            asset_records = self.physical_resolver.resolve(
                closure_ids=closure_ids,
                graph=graph_for_packaging,
                base_dir=base_dir,
                seed_ids=seed_ids,
                snapshot=result.snapshot,
            )
            
            pkg_id = hashlib.sha256(request.fingerprint().encode()).hexdigest()[:12]
            scene_name = result.snapshot.metadata.name if result.snapshot and result.snapshot.metadata else "scene"
            plan = self.m4_package_planner.create_plan(
                asset_records=asset_records,
                package_id=pkg_id,
                scene_name=scene_name,
                camera=request.camera_id,
                frame_start=request.frame_start,
                frame_end=request.frame_end,
            )
            result.package_plan = plan
            
            pkg_out_p = Path(request.package_output)
            pkg_dir = pkg_out_p.parent if pkg_out_p.suffix else pkg_out_p
            
            import os
            import shutil
            tmp_dir = pkg_dir.with_suffix('.tmp')
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)
                
            try:
                self.m4_package_builder.build_package(
                    plan,
                    output_dir=tmp_dir,
                    scene_source_path=request.input_path,
                    blender_executable=request.blender_executable,
                )
                result.package_integrity = self.package_validator.validate(plan, package_dir=tmp_dir)
                
                if result.package_integrity.verified:
                    if pkg_dir.exists():
                        shutil.rmtree(pkg_dir)
                    os.replace(tmp_dir, pkg_dir)
                    result.package_output_path = pkg_dir / "manifest.json"
                    result.messages.append(f"Physical package created at {pkg_dir}")
                else:
                    raise RuntimeError(f"Package validation failed. Failed assets: {[a.asset_id for a in result.package_integrity.failed_assets]}")
            finally:
                if tmp_dir.exists():
                    shutil.rmtree(tmp_dir)

        if cache is not None and source_hash is not None:"""

content = re.sub(
    r"        if request\.build_package:.*?        if cache is not None and source_hash is not None:",
    run_replacement,
    content,
    flags=re.DOTALL
)

# 4. Remove build_package and build_optimized_package
content = re.sub(
    r"    def build_package\(.*?def build_scheduling_plan\(",
    "    def build_scheduling_plan(",
    content,
    flags=re.DOTALL
)

f.write_text(content, "utf-8")
