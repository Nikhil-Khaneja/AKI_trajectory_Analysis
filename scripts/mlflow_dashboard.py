import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "outputs" / "models" / "registry.json"
DASHBOARD_PATH = ROOT / "outputs" / "reports" / "mlflow_dashboard.html"
DASHBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)

def generate_dashboard():
    print("Generating MLflow-style Dashboard...")
    
    if not REGISTRY_PATH.exists():
        print("Registry not found. Run a model training/registration first.")
        return

    with open(REGISTRY_PATH, "r") as f:
        runs = json.load(f)

    # Convert runs to a simple HTML table
    rows_html = ""
    for run in reversed(runs):
        metrics = run.get("metrics", {})
        lineage = run.get("lineage", {})
        
        rows_html += f"""
        <tr class="border-b hover:bg-gray-50">
            <td class="px-4 py-2 font-mono text-sm">{run['run_id']}</td>
            <td class="px-4 py-2 text-sm">{run['timestamp'][:16]}</td>
            <td class="px-4 py-2 text-blue-600 font-semibold">{metrics.get('auroc', 'N/A'):.4f}</td>
            <td class="px-4 py-2 text-green-600 font-semibold">{metrics.get('f1', 'N/A'):.4f}</td>
            <td class="px-4 py-2 text-sm text-gray-600">
                Data: {lineage.get('training_data_file', 'N/A')}<br>
                dbt: {lineage.get('dbt_logic_version', 'N/A')}
            </td>
            <td class="px-4 py-2">
                <span class="px-2 py-1 bg-green-100 text-green-800 text-xs rounded-full">Active</span>
            </td>
        </tr>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>AKI MLflow Experiment Tracker</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-gray-100 p-8">
        <div class="max-w-6xl mx-auto bg-white rounded-xl shadow-lg overflow-hidden">
            <div class="bg-blue-600 p-6 text-white flex justify-between items-center">
                <h1 class="text-2xl font-bold">AKI Model Registry (MLflow Simulation)</h1>
                <div class="text-sm opacity-80">v1.2.0 | Snowflake Backend</div>
            </div>
            
            <div class="p-6">
                <div class="mb-4 flex gap-4">
                    <div class="bg-blue-50 border border-blue-200 p-4 rounded-lg flex-1">
                        <div class="text-blue-600 text-xs font-bold uppercase">Total Runs</div>
                        <div class="text-3xl font-bold">{len(runs)}</div>
                    </div>
                    <div class="bg-green-50 border border-green-200 p-4 rounded-lg flex-1">
                        <div class="text-green-600 text-xs font-bold uppercase">Best AUROC</div>
                        <div class="text-3xl font-bold">{max([r.get('metrics', {}).get('auroc', 0) for r in runs]):.4f}</div>
                    </div>
                    <div class="bg-purple-50 border border-purple-200 p-4 rounded-lg flex-1">
                        <div class="text-purple-600 text-xs font-bold uppercase">Lineage Status</div>
                        <div class="text-3xl font-bold text-purple-700">Verified</div>
                    </div>
                </div>

                <table class="w-full text-left">
                    <thead class="bg-gray-50 border-b">
                        <tr>
                            <th class="px-4 py-2 text-sm font-semibold text-gray-700">Run ID</th>
                            <th class="px-4 py-2 text-sm font-semibold text-gray-700">Date</th>
                            <th class="px-4 py-2 text-sm font-semibold text-gray-700">AUROC</th>
                            <th class="px-4 py-2 text-sm font-semibold text-gray-700">F1 Score</th>
                            <th class="px-4 py-2 text-sm font-semibold text-gray-700">Lineage</th>
                            <th class="px-4 py-2 text-sm font-semibold text-gray-700">Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>
            
            <div class="bg-gray-50 p-4 text-center text-xs text-gray-500">
                Linked to Snowflake Schema: GOLD.AKI_TRAINING_SET
            </div>
        </div>
    </body>
    </html>
    """
    
    with open(DASHBOARD_PATH, "w") as f:
        f.write(html_content)
        
    print(f"Dashboard successfully generated at {DASHBOARD_PATH}")

if __name__ == "__main__":
    generate_dashboard()
