"""
전처리 스크립트
- 경기 카드소비 데이터
- 아파트 매매/전세 지수
- 가계지출 (추가 가능)
- 행정구역 코드 매핑
"""

import pandas as pd
import zipfile, glob, os, io, re

# ======================
# 1. 행정구역 코드 전처리
# ======================
def preprocess_admin_code(file_path: str, output_path: str):
    """법정동 행정구역코드에서 시군구 단위 코드만 추출"""
    df = pd.read_csv(file_path, sep="|", encoding="cp949", dtype=str)
    df_selected = df[["ADM_CD", "ADM_SECT_NM"]].drop_duplicates()
    df_selected["ADM_CD"] = df_selected["ADM_CD"].str[:8]
    df_sgg = df_selected[df_selected["ADM_CD"].str.endswith("00")]
    df_sgg = df_sgg.drop_duplicates()

    df_sgg.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ 행정구역 코드 저장 완료: {output_path}")
    return df_sgg


# ======================
# 2. 경기 카드소비 데이터 전처리
# ======================
def preprocess_card(zip_folder: str, code_map_path: str, output_path: str):
    """경기 카드소비 ZIP 데이터 → 시군구 단위 분기별 합산 + 정렬"""
    code_map = pd.read_csv(code_map_path, dtype=str)[["ADM_CD", "ADM_SECT_NM"]]
    code_map.rename(columns={"ADM_CD": "admi_cty_no"}, inplace=True)

    zip_files = glob.glob(os.path.join(zip_folder, "*.zip"))
    df_list = []

    for zf in zip_files:
        base = os.path.basename(zf)  # 카드소비 데이터_202401.zip
        year = int(base[-10:-6])
        month = int(base[-6:-4])
        quarter = (month - 1) // 3 + 1

        with zipfile.ZipFile(zf, "r") as zip_ref:
            for fname in zip_ref.namelist():
                if not fname.endswith(".csv"):
                    continue
                with zip_ref.open(fname) as f:
                    try:
                        temp = pd.read_csv(f, encoding="utf-8")
                    except:
                        try:
                            f.seek(0)
                            temp = pd.read_csv(f, encoding="cp949")
                        except:
                            f.seek(0)
                            temp = pd.read_csv(io.TextIOWrapper(f, encoding="latin1", errors="replace"))

                if "amt" not in temp.columns or "admi_cty_no" not in temp.columns:
                    continue

                grouped = temp.groupby("admi_cty_no", as_index=False)["amt"].sum()
                grouped["연도"] = year
                grouped["분기"] = quarter
                df_list.append(grouped)

    # ======================
    # 통합 DataFrame 생성
    # ======================
    df_final = pd.concat(df_list, ignore_index=True)

    # 코드 매핑
    df_final["admi_cty_no"] = df_final["admi_cty_no"].astype(str)
    code_map["admi_cty_no"] = code_map["admi_cty_no"].astype(str)
    df_final = df_final.merge(code_map, on="admi_cty_no", how="left")

    # ======================
    # 지역-연도-분기별 합산
    # ======================
    df_final = (
        df_final.groupby(["ADM_SECT_NM", "연도", "분기"], as_index=False)["amt"]
        .sum()
        .rename(columns={"ADM_SECT_NM": "지역", "amt": "소비금액"})
    )

    # 억 단위 컬럼 추가
    df_final["소비금액(억)"] = (df_final["소비금액"] / 1e8).round(1)

    # ======================
    # 정렬 (연도 → 분기 → 소비금액 내림차순)
    # ======================
    df_final = df_final.sort_values(
        by=["연도", "분기", "소비금액"], ascending=[True, True, False]
    ).reset_index(drop=True)

    # 저장
    df_final.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ 카드소비 데이터 저장 완료: {output_path}")
    return df_final



# ======================
# 3. 아파트 매매/전세 지수 전처리
# ======================
def preprocess_housing(file_path: str, output_path: str):
    """월별 아파트 매매/전세 지수 → 분기별 평균"""
    df = pd.read_excel(file_path)

    cols_2024 = [f"2024년 {i}월" for i in range(1, 13)]
    df_2024 = df.loc[:, ["지역"] + cols_2024].copy()

    for col in cols_2024:
        df_2024[col] = pd.to_numeric(df_2024[col], errors="coerce")

    df_2024["2024_1분기"] = df_2024[["2024년 1월","2024년 2월","2024년 3월"]].mean(axis=1)
    df_2024["2024_2분기"] = df_2024[["2024년 4월","2024년 5월","2024년 6월"]].mean(axis=1)
    df_2024["2024_3분기"] = df_2024[["2024년 7월","2024년 8월","2024년 9월"]].mean(axis=1)
    df_2024["2024_4분기"] = df_2024[["2024년 10월","2024년 11월","2024년 12월"]].mean(axis=1)

    df_final = df_2024[["지역","2024_1분기","2024_2분기","2024_3분기","2024_4분기"]]
    df_final.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ 아파트 지수 저장 완료: {output_path}")
    return df_final


# ======================
# 4. 실행
# ======================
if __name__ == "__main__":
    # 파일 경로 설정 (예시)
    base_dir = "C:/Users/User/OneDrive/ドキュメント\my_ws"
    admin_code_file = f"{base_dir}/법정동_행정구역 코드.txt"
    admin_code_out = f"{base_dir}/경기도_행정구역코드_시군구.csv"

    card_zip_dir = f"{base_dir}/카드소비데이터_모음"
    card_out = f"{base_dir}/경기_카드소비_시군구_분기별.csv"

    sales_file = f"{base_dir}/(월) 지역별 매매지수_아파트.xlsx"
    sales_out = f"{base_dir}/매매지수_2024분기.csv"

    rent_file = f"{base_dir}/(월) 지역별 전세지수_아파트.xlsx"
    rent_out = f"{base_dir}/전세지수_2024분기.csv"

    # 실행
    preprocess_admin_code(admin_code_file, admin_code_out)
    preprocess_card(card_zip_dir, admin_code_out, card_out)
    preprocess_housing(sales_file, sales_out)
    preprocess_housing(rent_file, rent_out)

    print("✅ 모든 전처리 완료")