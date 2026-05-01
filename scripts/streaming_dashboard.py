import streamlit as st
import subprocess, threading, time, os, sys, json, signal, queue
import pandas as pd
import numpy as np

st.set_page_config(page_title="AKI Command Center", page_icon="🧬", layout="wide")
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono&display=swap');
.stApp{background:#020617;color:#f1f5f9;font-family:'Inter',sans-serif;}
.sc{background:#0f172a;border:1px solid #1e293b;padding:12px;border-radius:10px;margin-bottom:6px;}
.sl{font-size:9px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:1px;}
.sv{font-size:18px;font-weight:700;color:#22d3ee;margin-top:2px;}
.tc{background:#0f172a;border-left:4px solid #22d3ee;padding:8px 12px;margin-bottom:5px;border-radius:0 6px 6px 0;font-size:11px;}
.tw{background:#1e293b;border-left:3px solid #7c3aed;padding:7px 10px;margin-bottom:4px;border-radius:0 6px 6px 0;font-size:10px;}
.ab{background:#000;border:1px solid #1e293b;padding:10px;border-radius:6px;font-family:'JetBrains Mono',monospace;font-size:11px;color:#4ade80;}
.vb{background:#000;border:1px solid #1e293b;padding:10px;border-radius:6px;font-family:'JetBrains Mono',monospace;font-size:10px;color:#22d3ee;}
.lb{background:#000;border:1px solid #1e293b;padding:10px;border-radius:6px;height:380px;overflow-y:auto;font-family:'JetBrains Mono',monospace;font-size:10px;}
</style>""", unsafe_allow_html=True)

class PM:
    def __init__(self):
        self.procs={}; self.logs=queue.Queue()
        self.running={"producer":False,"consumer":False}
    def start(self,name,cmd):
        if self.running[name]: return
        p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                           text=True,cwd=os.getcwd(),preexec_fn=os.setsid)
        self.procs[name]=p; self.running[name]=True
        def _r():
            for l in iter(p.stdout.readline,""):
                self.logs.put((name,l.rstrip()))
            self.running[name]=False
        threading.Thread(target=_r,daemon=True).start()
    def stop(self,name):
        if not self.running[name]: return
        try: os.killpg(os.getpgid(self.procs[name].pid),signal.SIGTERM)
        except: pass
        self.running[name]=False

if "pm"  not in st.session_state: st.session_state.pm=PM()
if "pts" not in st.session_state: st.session_state.pts={}
if "rl"  not in st.session_state: st.session_state.rl=[]
pm=st.session_state.pm

while not pm.logs.empty():
    src,line=pm.logs.get()
    st.session_state.rl.insert(0,(time.strftime("%H:%M:%S"),src,line))
    if len(st.session_state.rl)>500: st.session_state.rl=st.session_state.rl[:500]
    if "[PATIENT_TRACE]" in line:
        try:
            t=json.loads(line.split("[PATIENT_TRACE]",1)[1].strip())
            pid=t["patient_id"]
            if pid not in st.session_state.pts: st.session_state.pts[pid]=[]
            st.session_state.pts[pid].insert(0,t)
            if len(st.session_state.pts[pid])>100: st.session_state.pts[pid]=st.session_state.pts[pid][:100]
        except: pass

# Header
n_pts=len(st.session_state.pts)
n_evts=sum(len(v) for v in st.session_state.pts.values())
p_on="🟢 LIVE" if pm.running["producer"] else "⚫ OFF"
c_on="🟢 LIVE" if pm.running["consumer"] else "⚫ OFF"
st.markdown(f"""<div style="display:flex;justify-content:space-between;align-items:center;
border-bottom:1px solid #1e293b;padding-bottom:16px;margin-bottom:20px;">
<div style="display:flex;align-items:center;gap:12px;">
  <div style="background:#06b6d4;padding:10px;border-radius:8px;font-size:20px;">🧬</div>
  <div><div style="font-size:22px;font-weight:700;">AKI <span style="color:#22d3ee;">COMMAND CENTER</span></div>
  <div style="font-size:9px;color:#64748b;text-transform:uppercase;letter-spacing:2px;">Real-time Clinical Trace &amp; XAI</div></div>
</div>
<div style="display:flex;gap:12px;">
  <div class="sc" style="text-align:center;min-width:90px;"><div class="sl">Patients</div><div class="sv">{n_pts}</div></div>
  <div class="sc" style="text-align:center;min-width:90px;"><div class="sl">Events</div><div class="sv" style="color:#a78bfa;">{n_evts}</div></div>
  <div class="sc" style="text-align:center;min-width:100px;"><div class="sl">Producer</div><div style="font-size:13px;font-weight:600;margin-top:2px;">{p_on}</div></div>
  <div class="sc" style="text-align:center;min-width:100px;"><div class="sl">Consumer</div><div style="font-size:13px;font-weight:600;margin-top:2px;">{c_on}</div></div>
</div></div>""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### ⚙️ Controls")
    venv=sys.executable
    if not pm.running["producer"]:
        if st.button("▶ Start Producer",use_container_width=True):
            pm.start("producer",[venv,"spark/streaming/kafka_producer.py"]); st.rerun()
    else:
        if st.button("⏹ Stop Producer",use_container_width=True,type="primary"):
            pm.stop("producer"); st.rerun()
    st.markdown("---")
    if not pm.running["consumer"]:
        if st.button("▶ Start Spark Consumer",use_container_width=True):
            pm.start("consumer",[venv,"spark/streaming/streaming_job.py"]); st.rerun()
    else:
        if st.button("⏹ Stop Consumer",use_container_width=True,type="primary"):
            pm.stop("consumer"); st.rerun()
    st.markdown("---")
    if st.button("🧹 Clear Data",use_container_width=True):
        st.session_state.pts={}; st.session_state.rl=[]; st.rerun()
    if st.button("🔄 Reset Checkpoints",use_container_width=True):
        import shutil
        p=os.path.join("tmp","checkpoints")
        if os.path.exists(p): shutil.rmtree(p); st.success("Cleared!")

# ── 3 TOP-LEVEL TABS ──────────────────────────────────────────────────────────
tab_stream, tab_batch, tab_arch = st.tabs(["⚡ STREAMING","📊 BATCH ANALYSIS","🕸️ ARCHITECTURE"])

# ════════════════════════════════════════════════════════════════════════
with tab_stream:
    s1, s2, s3 = st.tabs(["🔴 LIVE PATIENT TRACES","⚙️ ALGORITHM DEEP DIVE","📜 RAW LOGS"])

    # ── LIVE PATIENT TRACES ───────────────────────────────────────────────
    with s1:
        pids=sorted(st.session_state.pts.keys())
        if not pids:
            st.markdown("""<div style="text-align:center;padding:60px;color:#475569;">
            <div style="font-size:48px;">⏳</div>
            <div style="font-size:18px;font-weight:600;margin-top:12px;">Awaiting Kafka Stream</div>
            <div style="font-size:12px;margin-top:8px;">Start Producer + Consumer from the sidebar</div>
            </div>""", unsafe_allow_html=True)
        else:
            left,right=st.columns([1,3])
            with left:
                st.markdown("#### Active Patients")
                sel=st.radio("",pids,label_visibility="collapsed")
                kc={0:"#22c55e",1:"#84cc16",2:"#f59e0b",3:"#ef4444"}
                for pid in pids:
                    lt=st.session_state.pts[pid][0]
                    s=lt.get("anomaly_score",0); k=lt.get("kdigo_stage",0)
                    rc="#ef4444" if s>=60 else "#f59e0b" if s>=30 else "#22c55e"
                    bc="#22d3ee" if pid==sel else "#1e293b"
                    st.markdown(f"""<div class="tc" style="border-left-color:{bc};margin-bottom:6px;">
                    <div style="font-weight:700;font-size:12px;">PID {pid}</div>
                    <div style="font-size:10px;color:#64748b;">{len(st.session_state.pts[pid])} events</div>
                    <div style="font-size:11px;margin-top:3px;">
                      <span style="color:{rc};">Risk {s}%</span>&nbsp;|&nbsp;
                      <span style="color:{kc.get(k,'#64748b')};">KDIGO {k}</span>
                    </div></div>""", unsafe_allow_html=True)

            with right:
                traces=st.session_state.pts[sel]; lt=traces[0]
                score=lt.get("anomaly_score",0); kdigo=lt.get("kdigo_stage",0)
                algos=lt.get("algorithms",{}); feats=lt.get("features",{})
                vector=lt.get("vector",[]); twins=lt.get("twins",[]); rec=lt.get("record",{})
                kc={0:"#22c55e",1:"#84cc16",2:"#f59e0b",3:"#ef4444"}
                rc2="#ef4444" if score>=60 else "#f59e0b" if score>=30 else "#22c55e"

                st.markdown(f"### Patient {sel}")
                c1,c2,c3,c4,c5=st.columns(5)
                def sc(label,val,color="#22d3ee"):
                    return f'<div class="sc"><div class="sl">{label}</div><div class="sv" style="color:{color};">{val}</div></div>'
                c1.markdown(sc("Anomaly",f"{score}%",rc2),unsafe_allow_html=True)
                c2.markdown(sc("KDIGO",f"Stage {kdigo}",kc.get(kdigo,"#64748b")),unsafe_allow_html=True)
                c3.markdown(sc("Bloom","UNIQUE","#4ade80"),unsafe_allow_html=True)
                c4.markdown(sc("FM Patients ≈",algos.get("fm_distinct_patients","?")),unsafe_allow_html=True)
                c5.markdown(sc("Privacy",algos.get("k_anonymity","?"),"#a78bfa"),unsafe_allow_html=True)

                st.markdown("<br>",unsafe_allow_html=True)
                r1,r2,r3=st.columns(3)
                with r1:
                    st.markdown("##### 📨 Kafka Record")
                    st.markdown(f"""<div class="ab">TOPIC: aki.live.events<br>──────────────────<br>
type  : <span style="color:#f59e0b;">{rec.get('type','?')}</span><br>
value : <span style="color:#22d3ee;">{rec.get('value','?')} {rec.get('uom','')}</span><br>
time  : {lt.get('timestamp','?')}<br>batch : #{lt.get('batch_id','?')}</div>""",unsafe_allow_html=True)
                with r2:
                    st.markdown("##### ⚙️ Algorithms")
                    st.markdown(f"""<div class="ab">BLOOM FILTER<br>
&nbsp; result : <span style="color:#4ade80;">UNIQUE</span><br><br>
FLAJOLET-MARTIN<br>
&nbsp; ≈ <span style="color:#22d3ee;">{algos.get('fm_distinct_patients','?')} patients</span><br><br>
DGIM (1h window)<br>
&nbsp; ≈ <span style="color:#a78bfa;">{algos.get('dgim_events_1h','?')} events</span><br><br>
K-ANONYMITY<br>
&nbsp; group : <span style="color:#f59e0b;">{algos.get('k_anonymity','?')}</span></div>""",unsafe_allow_html=True)
                with r3:
                    st.markdown("##### 🔢 Feature Vector")
                    lbls=["cr","base_cr","ratio","Δcr","Δ48h","uo_6h","uo_24h","age","sex","unit"]
                    rows="<br>".join(
                        f'<span style="color:#64748b;">{lbls[i] if i<len(lbls) else i:>8}</span>'
                        f' : <span style="color:#22d3ee;">{v}</span>'
                        for i,v in enumerate(vector)
                    )
                    st.markdown(f'<div class="vb">{rows}</div>',unsafe_allow_html=True)

                st.markdown("<br>",unsafe_allow_html=True)
                f1,f2=st.columns(2)
                with f1:
                    st.markdown("##### 📈 Clinical Features")
                    lblmap={"creatinine":"Creatinine (mg/dL)","baseline_cr":"Baseline Cr","cr_ratio":"Cr/Baseline Ratio",
                            "cr_delta_recent":"Cr Delta","uo_ml_kg_hr_6h":"UO 6h (mL/kg/hr)","uo_ml_kg_hr_24h":"UO 24h (mL/kg/hr)"}
                    for k,v in feats.items():
                        flag=""
                        if k=="cr_ratio" and isinstance(v,(int,float)) and v>=1.5: flag=" ⚠️"
                        if k=="uo_ml_kg_hr_6h" and isinstance(v,(int,float)) and v<0.5: flag=" 🔴"
                        st.markdown(f'<div style="display:flex;justify-content:space-between;font-size:11px;'
                                    f'padding:4px 0;border-bottom:1px solid #1e293b;">'
                                    f'<span style="color:#94a3b8;">{lblmap.get(k,k)}</span>'
                                    f'<span style="color:#f1f5f9;font-weight:600;">{v}{flag}</span></div>',
                                    unsafe_allow_html=True)
                with f2:
                    st.markdown("##### 🧠 XAI: LSH Clinical Twins")
                    st.markdown('<p style="font-size:10px;color:#64748b;">Approximate Nearest Neighbors from historical eICU cohort</p>',unsafe_allow_html=True)
                    if not twins:
                        st.warning("No twins found — LSH index may still be loading.")
                    else:
                        for i,tw in enumerate(twins):
                            oc="#ef4444" if "Stage 3" in tw.get("outcome","") else "#22c55e"
                            st.markdown(f"""<div class="tw">
                            <div style="display:flex;justify-content:space-between;">
                              <b>#{i+1} Patient {tw['id']}</b>
                              <span style="color:#7c3aed;">Dist: {tw['dist']}</span>
                            </div>
                            <div style="color:{oc};margin-top:2px;">{tw.get('outcome','?')}</div>
                            <div style="color:#64748b;font-size:9px;">Cr:{tw.get('cr','?')} | UO:{tw.get('uo','?')} mL</div>
                            </div>""",unsafe_allow_html=True)

                st.markdown("##### 🕐 Event Timeline")
                for t in traces:
                    s2v=t.get("anomaly_score",0); k2=t.get("kdigo_stage",0); r2v=t.get("record",{})
                    rc3="#ef4444" if s2v>=60 else "#f59e0b" if s2v>=30 else "#22c55e"
                    st.markdown(f"""<div class="tc">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                      <div><span style="color:#64748b;font-size:9px;">{t.get('timestamp','?')}</span>
                        &nbsp;<b>{r2v.get('type','?')}</b>
                        &nbsp;<span style="color:#22d3ee;">{r2v.get('value','?')} {r2v.get('uom','')}</span></div>
                      <div style="font-size:10px;display:flex;gap:8px;">
                        <span style="color:{rc3};">Risk {s2v}%</span>
                        <span style="color:{kc.get(k2,'#64748b')};">KDIGO {k2}</span>
                        <span style="color:#64748b;">B#{t.get('batch_id','?')}</span>
                      </div>
                    </div></div>""",unsafe_allow_html=True)

    # ── ALGORITHM DEEP DIVE ───────────────────────────────────────────────
    with s2:
        st.markdown("### Algorithm Deep Dive — All Active Patients")
        if not st.session_state.pts:
            st.info("No data yet — start streaming first.")
        else:
            rows=[]
            for pid,trs in st.session_state.pts.items():
                lt=trs[0]
                rows.append({"Patient ID":pid,"Events":len(trs),
                    "KDIGO Stage":lt.get("kdigo_stage",0),
                    "Anomaly %":lt.get("anomaly_score",0),
                    "Last Type":lt.get("record",{}).get("type","?"),
                    "Last Value":lt.get("record",{}).get("value","?"),
                    "Cr Ratio":lt.get("features",{}).get("cr_ratio","?"),
                    "UO 6h":lt.get("features",{}).get("uo_ml_kg_hr_6h","?"),
                    "FM Est":lt.get("algorithms",{}).get("fm_distinct_patients","?"),
                    "DGIM 1h":lt.get("algorithms",{}).get("dgim_events_1h","?"),
                    "LSH Twins":len(lt.get("twins",[])),
                    "K-Anon":lt.get("algorithms",{}).get("k_anonymity","?")})
            df=pd.DataFrame(rows).sort_values("Anomaly %",ascending=False)
            st.dataframe(df.style.background_gradient(subset=["Anomaly %"],cmap="RdYlGn_r"),
                         use_container_width=True,hide_index=True)
            st.markdown("#### Anomaly Score by Patient")
            st.bar_chart(df.set_index("Patient ID")[["Anomaly %"]])

    # ── RAW LOGS ──────────────────────────────────────────────────────────
    with s3:
        st.markdown("### Raw Streaming Logs")
        html='<div class="lb">'
        for ts,src,msg in st.session_state.rl[:300]:
            c="#f97316" if src=="producer" else "#06b6d4"
            safe=msg.replace("<","&lt;").replace(">","&gt;")
            html+=f'<div><span style="color:#475569;">[{ts}]</span> <span style="color:{c};">[{src.upper()}]</span> {safe}</div>'
        html+='</div>'
        st.markdown(html,unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════════
with tab_batch:
    st.markdown("## 📊 Model Registry & Analytical Outputs")
    
    # 1. Model Registry from outputs/models/registry.json
    try:
        with open("outputs/models/registry.json", "r") as f:
            registry = json.load(f)
        
        st.markdown("### 📜 Model Performance Registry")
        for model in registry:
            with st.expander(f"Model: {model['model_name']} ({model['timestamp']})", expanded=True):
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("AUROC", f"{model['metrics']['auroc']:.4f}")
                col2.metric("AUPRC", f"{model['metrics']['auprc']:.4f}")
                col3.metric("F1 Score", f"{model['metrics']['f1']:.4f}")
                col4.metric("Accuracy", f"{model['metrics']['accuracy']:.4f}")
                
                st.markdown("**Parameters:** " + ", ".join([f"`{k}: {v}`" for k, v in model['parameters'].items()]))
                st.markdown(f"**Lineage:** Logic: `{model['lineage']['dbt_logic_version']}` | Data: `{model['lineage']['training_data_file']}`")
    except Exception as e:
        st.warning(f"Could not load model registry: {e}")

    st.markdown("---")
    
    # 2. Confusion Matrices & Evaluation Visuals
    st.markdown("### 📉 Comprehensive Evaluation Suite")
    
    # ROC and LR/GBT Confusion Matrices
    row1_c1, row1_c2, row1_c3 = st.columns(3)
    with row1_c1:
        st.markdown("**ROC Curves (Comparative)**")
        if os.path.exists("outputs/metrics/roc_curves.png"):
            st.image("outputs/metrics/roc_curves.png", use_container_width=True)
    with row1_c2:
        st.markdown("**GBT Confusion Matrix**")
        if os.path.exists("docs/reports/model_analysis/confusion_matrix_gradient_boosting.png"):
            st.image("docs/reports/model_analysis/confusion_matrix_gradient_boosting.png", use_container_width=True)
    with row1_c3:
        st.markdown("**LR Confusion Matrix**")
        if os.path.exists("docs/reports/model_analysis/confusion_matrix_logistic_regression.png"):
            st.image("docs/reports/model_analysis/confusion_matrix_logistic_regression.png", use_container_width=True)

    # Correlation and Covariance
    st.markdown("<br>", unsafe_allow_html=True)
    row2_c1, row2_c2 = st.columns(2)
    with row2_c1:
        st.markdown("**Feature Correlation Matrix**")
        if os.path.exists("docs/reports/model_analysis/correlation_matrix.png"):
            st.image("docs/reports/model_analysis/correlation_matrix.png", use_container_width=True)
    with row2_c2:
        st.markdown("**Feature Covariance Matrix**")
        if os.path.exists("docs/reports/model_analysis/covariance_matrix.png"):
            st.image("docs/reports/model_analysis/covariance_matrix.png", use_container_width=True)

    st.markdown("---")
    
    # 3. Model Metric CSV Summaries
    st.markdown("### 📋 Detailed Metric Summaries")
    m_col1, m_col2 = st.columns(2)
    with m_col1:
        st.markdown("**Gradient Boosting Metrics**")
        if os.path.exists("outputs/metrics/gradient_boosting_metrics.csv"):
            st.dataframe(pd.read_csv("outputs/metrics/gradient_boosting_metrics.csv"), use_container_width=True, hide_index=True)
    with m_col2:
        st.markdown("**Logistic Regression Metrics**")
        if os.path.exists("outputs/metrics/logistic_regression_metrics.csv"):
            st.dataframe(pd.read_csv("outputs/metrics/logistic_regression_metrics.csv"), use_container_width=True, hide_index=True)

    st.markdown("---")
    
    # 3. Feature Distributions from docs/reports/feature_distributions
    st.markdown("### 📈 Clinical Feature Distributions")
    f1, f2, f3 = st.columns(3)
    dist_imgs = {
        "Creatinine": "docs/reports/feature_distributions/distribution_creatinine.png",
        "Urine Output": "docs/reports/feature_distributions/distribution_urine.png",
        "Hours since Admit": "docs/reports/feature_distributions/distribution_hours.png"
    }
    cols = [f1, f2, f3]
    for i, (label, path) in enumerate(dist_imgs.items()):
        with cols[i]:
            st.markdown(f"**{label}**")
            if os.path.exists(path):
                st.image(path, use_container_width=True)
            else:
                st.info(f"{label} distribution not found.")

    st.markdown("---")
    
    # 4. Mart Data Preview
    st.markdown("### 💎 Data Mart Preview (Sample Metrics)")
    try:
        if os.path.exists("outputs/mart/dp_unit_metrics.csv"):
            df_mart = pd.read_csv("outputs/mart/dp_unit_metrics.csv")
            st.dataframe(df_mart, use_container_width=True, hide_index=True)
    except:
        st.info("Mart metrics CSV not found.")

# ════════════════════════════════════════════════════════════════════════
with tab_arch:
    st.markdown("## 🧬 End-to-End Clinical Architecture")
    
    st.markdown("""
    <style>
    @keyframes fadeInUp {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    @keyframes scaleIn {
        from { transform: scale(0.9); opacity: 0; }
        to { transform: scale(1); opacity: 1; }
    }
    @keyframes drawLine {
        from { height: 0; }
        to { height: 60px; }
    }
    .tree-wrapper { 
        display: flex; flex-direction: column; align-items: center; 
        padding: 40px; background: #020617; border-radius: 20px; 
        font-family: 'Inter', sans-serif; overflow: hidden;
    }
    
    .root-node { 
        background: linear-gradient(135deg, #22d3ee, #06b6d4); color: #020617; 
        padding: 22px 45px; border-radius: 40px; font-weight: 900; font-size: 1.3rem; 
        position: relative; z-index: 10; box-shadow: 0 0 30px rgba(34, 211, 238, 0.4);
        animation: scaleIn 0.8s cubic-bezier(0.16, 1, 0.3, 1) forwards;
        border: 2px solid #fff;
    }
    .root-node::after { 
        content: ''; position: absolute; top: 100%; left: 50%; width: 2px; 
        height: 60px; background: #334155; animation: drawLine 1s ease forwards;
    }
    
    .split-container { display: flex; width: 100%; justify-content: space-between; position: relative; margin-top: 60px; }
    .split-container::before { 
        content: ''; position: absolute; top: 0; left: 15%; right: 15%; height: 2px; background: #334155;
    }

    .tree-branch { width: 45%; display: flex; flex-direction: column; align-items: center; position: relative; }
    .tree-branch::before { content: ''; position: absolute; top: 0; width: 2px; height: 30px; background: #334155; }
    .left-branch::before { left: 50%; }
    .right-branch::before { right: 50%; }

    .branch-tag { 
        margin-top: 30px; padding: 12px 25px; border-radius: 12px; 
        font-weight: 800; font-size: 0.9rem; letter-spacing: 1px; margin-bottom: 35px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3); border: 1px solid rgba(255,255,255,0.1);
        animation: fadeInUp 0.8s ease 0.2s forwards; opacity: 0;
    }
    .batch-tag { background: #1e3a8a; color: #93c5fd; }
    .stream-tag { background: #581c87; color: #e9d5ff; }

    .node-card { 
        background: #0f172a; border: 1px solid #1e293b; padding: 22px; 
        border-radius: 18px; width: 100%; margin-bottom: 45px; position: relative; 
        transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
        animation: fadeInUp 0.8s ease forwards; opacity: 0;
    }
    .node-card:hover { 
        border-color: #22d3ee; transform: translateY(-8px) scale(1.02); 
        box-shadow: 0 15px 40px rgba(0,0,0,0.6); z-index: 5;
    }
    .node-card::after { 
        content: ''; position: absolute; top: 100%; left: 50%; width: 2px; height: 45px; background: #334155;
    }
    .node-card:last-child::after { display: none; }

    .node-header { display: flex; align-items: center; gap: 15px; margin-bottom: 14px; }
    .node-icon { font-size: 26px; }
    .node-name { font-weight: 700; color: #f1f5f9; font-size: 1.05rem; }
    .node-body { color: #94a3b8; font-size: 0.88rem; line-height: 1.6; }
    .node-tech { 
        display: inline-block; margin-top: 12px; padding: 4px 10px; border-radius: 6px; 
        background: #1e293b; color: #22d3ee; font-size: 0.75rem; font-weight: 700; 
        text-transform: uppercase; letter-spacing: 0.5px; border: 1px solid rgba(34, 211, 238, 0.2);
    }
    
    /* Staggered animations for cards */
    .tree-branch .node-card:nth-child(2) { animation-delay: 0.4s; }
    .tree-branch .node-card:nth-child(4) { animation-delay: 0.6s; }
    .tree-branch .node-card:nth-child(6) { animation-delay: 0.8s; }
    .tree-branch .node-card:nth-child(8) { animation-delay: 1.0s; }
    .tree-branch .node-card:nth-child(10) { animation-delay: 1.2s; }
    </style>
    
    <div class="tree-wrapper">
        <div class="root-node">🏥 MIMIC-IV CLINICAL DATA SOURCE</div>
        
        <div class="split-container">
            <!-- BATCH BRANCH (LEFT) -->
            <div class="tree-branch left-branch">
                <div class="branch-tag batch-tag">BATCH ANALYTICAL PIPELINE</div>
                
                <div class="node-card">
                    <div class="node-header"><span class="node-icon">✅</span><span class="node-name">Data Quality Gate</span></div>
                    <div class="node-body">Multi-stage validation of clinical feeds. Implements checks for nulls, referential integrity, and physiological bounds (e.g., Creatinine 0.1-20 mg/dL).</div>
                    <div class="node-tech">Great Expectations</div>
                </div>
                
                <div class="node-card">
                    <div class="node-header"><span class="node-icon">🥉</span><span class="node-name">Bronze (Raw Validated)</span></div>
                    <div class="node-body">Source-of-truth storage for validated raw events. Maintains high-fidelity data history for regulatory compliance and audit trails.</div>
                    <div class="node-tech">Snowflake + Parquet</div>
                </div>

                <div class="node-card">
                    <div class="node-header"><span class="node-icon">🥈</span><span class="node-name">Silver (Clinical Clean)</span></div>
                    <div class="node-body">Normalizes heterogeneous units (e.g., mL vs L), aligns timestamps across different monitors, and maps patient IDs to ICU stay windows.</div>
                    <div class="node-tech">dbt-snowflake</div>
                </div>

                <div class="node-card">
                    <div class="node-header"><span class="node-icon">🥇</span><span class="node-name">Gold (Clinical Features)</span></div>
                    <div class="node-body">Reifies KDIGO 2012 staging logic. Computes baseline creatinine, Cr ratio, and rolling 6h/24h urine output windows per kg/hr.</div>
                    <div class="node-tech">PySpark Engine</div>
                </div>

                <div class="node-card">
                    <div class="node-header"><span class="node-icon">🤖</span><span class="node-name">Model Training & Registry</span></div>
                    <div class="node-body">Trains GBT and LR models with lineage tracking. Persists metrics and model artifacts to the registry for downstream streaming inference.</div>
                    <div class="node-tech">Spark MLlib + MLflow</div>
                </div>
            </div>

            <!-- STREAMING BRANCH (RIGHT) -->
            <div class="tree-branch right-branch">
                <div class="branch-tag stream-tag">REAL-TIME WARNING SYSTEM</div>
                
                <div class="node-card">
                    <div class="node-header"><span class="node-icon">📨</span><span class="node-name">Kafka Event Ingress</span></div>
                    <div class="node-body">Replays clinical events from the Silver layer in chronological order. Simulates live ICU telemetry across multiple patient bedside monitors.</div>
                    <div class="node-tech">Kafka Clusters</div>
                </div>

                <div class="node-card">
                    <div class="node-header"><span class="node-icon">🔄</span><span class="node-name">Spark Micro-Batching</span></div>
                    <div class="node-body">Processes incoming Kafka records with low latency. Maintains stateful patient windows to update feature vectors in real-time.</div>
                    <div class="node-tech">Structured Streaming</div>
                </div>

                <div class="node-card">
                    <div class="node-header"><span class="node-icon">🧬</span><span class="node-name">Streaming Estimators</span></div>
                    <div class="node-body">Implements Bloom (Dedup), Flajolet-Martin (Patient Counting), and DGIM (Windowed Counting) for efficient stream analysis.</div>
                    <div class="node-tech">Approximation Algos</div>
                </div>

                <div class="node-card">
                    <div class="node-header"><span class="node-icon">🔍</span><span class="node-name">XAI: LSH Twin Search</span></div>
                    <div class="node-body">Queries the Gold layer using Locality Sensitive Hashing to find 'Clinical Twins' and explain predicted risk via historical similarity.</div>
                    <div class="node-tech">LSH / FAISS</div>
                </div>

                <div class="node-card">
                    <div class="node-header"><span class="node-icon">🚨</span><span class="node-name">Live Warning Dashboard</span></div>
                    <div class="node-body">Final UI for proactive monitoring. Triggers alerts and displays per-patient AKI progression risk 48 hours before physiological stage shifts.</div>
                    <div class="node-tech">Streamlit / Plotly</div>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

if pm.running["producer"] or pm.running["consumer"]:
    time.sleep(1); st.rerun()
