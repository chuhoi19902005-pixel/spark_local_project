import os

java_home = os.environ.get("JAVA_HOME")
print("==================================================")
print("1. 您目前系統中的 JAVA_HOME 變數值為:")
print(f"   {repr(java_home)}")
print("--------------------------------------------------")

if java_home:
    # 去除可能不小心夾帶的雙引號
    clean_path = java_home.replace('"', '')
    java_exe = os.path.join(clean_path, "bin", "java.exe")

    print("2. Spark 預期會去這裡尋找 java.exe:")
    print(f"   {java_exe}")
    print("--------------------------------------------------")

    print("3. 這個 java.exe 檔案真的存在嗎?")
    if os.path.exists(java_exe):
        print("   ✅ 存在！路徑正確。")
    else:
        print("   ❌ 不存在！這就是導致 JAVA_GATEWAY_EXITED 的原因。")

        # 幫忙檢查父目錄是否存在
        if os.path.exists(clean_path):
            print(f"   (資料夾 {clean_path} 存在，但裡面沒有 bin\\java.exe)")
        else:
            print(f"   (連 {clean_path} 這個資料夾都找不到，請檢查是否有錯字)")
else:
    print("   ❌ 系統中完全找不到 JAVA_HOME 變數，請重新設定。")
print("==================================================")