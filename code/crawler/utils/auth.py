 
import requests
import pickle
import os
from loguru import logger
from crawler.env import CRAWL_DATA_LVN_NAME, CRAWL_DATA_LVN_PASS

COOKIE_FILE = os.path.join(os.path.dirname(__file__), "session_cookies.pkl")

def get_authenticated_session():
    """
    Trả về session đã đăng nhập, tự động xử lý cookie
    """
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://luatvietnam.vn/'
    })

    # Nếu có cookie cũ, load vào
    if os.path.exists(COOKIE_FILE):
        try:
            with open(COOKIE_FILE, "rb") as f:
                cookies = pickle.load(f)
                session.cookies.update(cookies)
            logger.info("✅ Đã nạp Cookie từ file.")
            
            # Kiểm tra cookie còn hiệu lực không bằng cách test 1 request
            test_response = session.get("https://luatvietnam.vn/")
            if test_response.status_code == 200:
                # Kiểm tra xem có đăng nhập không (tùy thuộc vào cấu trúc trang)
                if "đăng xuất" in test_response.text.lower() or "tài khoản của tôi" in test_response.text.lower():
                    logger.success("✅ Cookie còn hiệu lực, đã đăng nhập.")
                    return session
                else:
                    logger.warning("⚠️ Cookie hết hạn, cần đăng nhập lại.")
        except Exception as e:
            logger.warning(f"⚠️ Lỗi khi load cookie: {e}. Sẽ đăng nhập lại.")

    # Nếu chưa có hoặc hết hạn, đăng nhập mới
    login_url = "https://luatvietnam.vn/Account/DoLogin"
    payload = {
        "CustomerName": CRAWL_DATA_LVN_NAME,
        "CustomerPass": CRAWL_DATA_LVN_PASS,
        "RememberMe": "true"
    }

    try:
        logger.info("🔐 Đang đăng nhập...")
        login_response = session.post(login_url, data=payload)
        
        if login_response.status_code == 200:
            # Kiểm tra phản hồi JSON
            try:
                response_json = login_response.json()
                if response_json.get('Success') or response_json.get('success'):
                    logger.success("✅ Đăng nhập thành công!")
                else:
                    logger.error(f"❌ Đăng nhập thất bại: {response_json.get('Message', 'Unknown error')}")
            except:
                # Nếu không phải JSON, kiểm tra text
                logger.success("✅ Đăng nhập thành công (response không phải JSON)!")
            
            # Lưu cookie
            with open(COOKIE_FILE, "wb") as f:
                pickle.dump(session.cookies, f)
            logger.info("💾 Đã lưu Cookie mới.")
        else:
            logger.error(f"❌ Đăng nhập thất bại: {login_response.status_code}")
            
    except Exception as e:
        logger.error(f"❌ Lỗi khi đăng nhập: {e}")

    return session

# tem8a4@gmail.com
# whynotnghiavu@gmail.com
