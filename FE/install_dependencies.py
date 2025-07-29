#!/usr/bin/env python3

import subprocess
import sys
import os

def install_package(package):
    """Install a package with error handling"""
    try:
        print(f"🔧 Installing {package}...")
        result = subprocess.run([sys.executable, "-m", "pip", "install", package], 
                              capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print(f"✅ {package} installed successfully")
            return True
        else:
            print(f"❌ Failed to install {package}")
            print(f"Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ Installation of {package} timed out")
        return False
    except Exception as e:
        print(f"❌ Error installing {package}: {e}")
        return False

def main():
    print("📦 Installing Streamlit Dashboard Dependencies")
    print("=" * 50)
    
    # Essential packages in order of dependency
    packages = [
        "numpy>=1.24.0",
        "requests>=2.28.0", 
        "streamlit>=1.28.0",
        "plotly>=5.15.0",
        "matplotlib>=3.7.0",
    ]
    
    # Try pandas separately with fallback
    pandas_packages = [
        "pandas>=2.0.0",
        "pandas==1.5.3",  # Fallback for compatibility issues
    ]
    
    failed_packages = []
    
    # Install core packages
    for package in packages:
        if not install_package(package):
            failed_packages.append(package)
    
    # Try pandas with fallbacks
    pandas_installed = False
    for pandas_pkg in pandas_packages:
        if install_package(pandas_pkg):
            pandas_installed = True
            break
    
    if not pandas_installed:
        failed_packages.append("pandas")
    
    # Try seaborn (optional)
    if not install_package("seaborn>=0.12.0"):
        print("⚠️  Seaborn failed to install (optional dependency)")
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Installation Summary")
    print("=" * 50)
    
    if failed_packages:
        print(f"❌ Failed packages: {', '.join(failed_packages)}")
        print("⚠️  You may need to install these manually or use different versions")
    else:
        print("✅ All packages installed successfully!")
    
    print("\n🚀 To start the dashboard:")
    print("1. Make sure API server is running: python api_server.py")
    print("2. Run: streamlit run FE/streamlit_fe.py")
    print("3. Open: http://localhost:8501")

if __name__ == "__main__":
    main()