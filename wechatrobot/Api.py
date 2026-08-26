from typing import Callable, Any, Union, Awaitable , Optional , Dict
import requests
import json
import os
import threading
from .Modles import *
import base64
from wechatrobot import ChatRoomData_pb2 as ChatRoom


OPENIM_CONTACT_SUFFIXES = ("@openim", "@kefu.openim")


def is_openim_contact_id(wxid: str) -> bool:
    return str(wxid or "").lower().endswith(OPENIM_CONTACT_SUFFIXES)


class Api:
    DB_HANDLE_ERRORS = {"database handle unavailable", "database query failed"}
    port: int = 18888
    db_handle: Dict[str, int] = 0
    request_timeout = None

    def __init__(self, port: int = 18888):
        self.port = self._get_port(port)
        self.api_base = self._get_api_base(self.port)
        self.db_handle = {}
        self._db_handle_lock = threading.Lock()
        self.request_timeout = self._env_float("WECHATROBOT_API_TIMEOUT", 5.0)
        self.send_timeout = self._env_float("WECHATROBOT_SEND_API_TIMEOUT", 60.0)

    @staticmethod
    def _env_float(name: str, default: float) -> float:
        value = os.environ.get(name)
        if value is None:
            return default
        try:
            parsed = float(value)
        except ValueError:
            return default
        return parsed if parsed > 0 else default

    def _get_port(self, default_port: int) -> int:
        value = os.environ.get("WECHATROBOT_API_PORT")
        if value is None:
            return default_port
        try:
            return int(value)
        except ValueError:
            return default_port

    def _get_api_base(self, port: int) -> str:
        base = os.environ.get("WECHATROBOT_API_BASE")
        if base:
            return base.rstrip("/")
        host = os.environ.get("WECHATROBOT_API_HOST", "127.0.0.1").strip() or "127.0.0.1"
        return f"http://{host}:{port}"

    def _api_url(self, api_type: int) -> str:
        return f"{self.api_base}/api/?type={api_type}"

    def IsLoginIn(self , **params) -> Dict:
        return self.post(WECHAT_IS_LOGIN , IsLoginBody(**params))

    def GetSelfInfo(self , **params) -> Dict:
        return self.post(WECHAT_GET_SELF_INFO , GetSelfInfoBody(**params))

    def SendText(self , **params) -> Dict:
        return self.post(WECHAT_MSG_SEND_TEXT , SendTextBody(**params), timeout=self.send_timeout)

    def SendAt(self , **params) -> Dict:
        return self.post(WECHAT_MSG_SEND_AT , SendAtBody(**params), timeout=self.send_timeout)

    def SendCard(self , **params) -> Dict:
        return self.post(WECHAT_MSG_SEND_CARD , SendCardBody(**params), timeout=self.send_timeout)

    def SendImage(self , **params) -> Dict:
        return self.post(WECHAT_MSG_SEND_IMAGE , SendImageBody(**params), timeout=self.send_timeout)

    def SendFile(self , **params) -> Dict:
        return self.post(WECHAT_MSG_SEND_FILE , SendFileBody(**params), timeout=self.send_timeout)
    
    def SendArticle(self , **params) -> Dict:
        return self.post(WECHAT_MSG_SEND_ARTICLE , SendArticleBody(**params), timeout=self.send_timeout)

    def SendApp(self , **params) -> Dict:
        return self.post(WECHAT_MSG_SEND_APP , SendAppBody(**params), timeout=self.send_timeout)

    def StartMsgHook(self, **params) -> Dict:
        return self.post(WECHAT_MSG_START_HOOK , StartMsgHookBody(**params))

    def StopMsgHook(self , **params) -> Dict:
        return self.post(WECHAT_MSG_STOP_HOOK , StopMsgHookBody(**params))
    
    def StartImageHook(self , **params) -> Dict:
        return self.post(WECHAT_MSG_START_IMAGE_HOOK , StartImageHookBody(**params))

    def StopImageHook(self , **params) -> Dict:
        return self.post(WECHAT_MSG_STOP_IMAGE_HOOK , StopImageHookBody(**params))

    def StartVoiceHook(self , **params) -> Dict:
        return self.post(WECHAT_MSG_START_VOICE_HOOK  , StartVoiceHookBody(**params))

    def StopVoiceHook(self , **params) -> Dict:
        return self.post(WECHAT_MSG_STOP_VOICE_HOOK , StopVoiceHookBody(**params))

    def GetContactList(self , **params) -> Dict:
        return self.post(WECHAT_CONTACT_GET_LIST , GetContactListBody(**params))

    def CheckContactStatus(self , **params) -> Dict:
        return self.post(WECHAT_CONTACT_CHECK_STATUS , CheckContactStatusBody(**params))

    def DelContact(self , **params) -> Dict:
        return self.post(WECHAT_CONTACT_DEL , DelContactBody(**params))

    def SearchContactByCache(self , **params) -> Dict:
        return self.post(WECHAT_CONTACT_SEARCH_BY_CACHE , SearchContactByCacheBody(**params))

    def SearchContactByNet(self , **params) -> Dict:
        return self.post(WECHAT_CONTACT_SEARCH_BY_NET , SearchContactByNetBody(**params))

    def AddContactByWxid(self , **params) -> Dict:
        return self.post(WECHAT_CONTACT_ADD_BY_WXID , AddContactByWxidBody(**params))

    def AddContactByV3(self , **params) -> Dict:
        return self.post(WECHAT_CONTACT_ADD_BY_V3 , AddContactByV3Body(**params))

    def AddContactByPublicId(self , **params) -> Dict:
        return self.post(WECHAT_CONTACT_ADD_BY_PUBLIC_ID , AddContactByPublicIdBody(**params))

    def VerifyApply(self , **params) -> Dict:
        return self.post(WECHAT_CONTACT_VERIFY_APPLY , VerifyApplyBody(**params))

    def EditRemark(self , **params) -> Dict:
        return self.post(WECHAT_CONTACT_EDIT_REMARK , EditRemarkBody(**params))

    def GetChatroomMemberList(self , **params) -> Dict:
        return self.post(WECHAT_CHATROOM_GET_MEMBER_LIST , GetChatroomMemberListBody(**params))

    def GetChatroomMemberNickname(self , **params) -> Dict:
        return self.post(WECHAT_CHATROOM_GET_MEMBER_NICKNAME , GetChatroomMemberNicknameBody(**params))

    def DelChatroomMember(self , **params) -> Dict:
        return self.post(WECHAT_CHATROOM_DEL_MEMBER , DelChatroomMemberBody(**params))

    def AddChatroomMember(self , **params) -> Dict:
        return self.post(WECHAT_CHATROOM_ADD_MEMBER , AddChatroomMemberBody(**params))

    def SetChatroomAnnouncement(self , **params) -> Dict:
        return self.post(WECHAT_CHATROOM_SET_ANNOUNCEMENT , SetChatroomAnnouncementBody(**params))

    def SetChatroomName(self , **params) -> Dict:
        return self.post(WECHAT_CHATROOM_SET_CHATROOM_NAME , SetChatroomNameBody(**params))

    def SetChatroomSelfNickname(self , **params) -> Dict:
        return self.post(WECHAT_CHATROOM_SET_SELF_NICKNAME , SetChatroomSelfNicknameBody(**params))

    def GetDatabaseHandles(self , **params) -> Dict:
        return self.post(WECHAT_DATABASE_GET_HANDLES , GetDatabaseHandlesBody(**params))

    def BackupDatabase(self , **params) -> Dict:
        return self.post(WECHAT_DATABASE_BACKUP , BackupDatabaseBody(**params))

    def QueryDatabase(self , **params) -> Dict:
        db_name = params.pop("db_name", None)
        db_handle = params.get("db_handle")
        if db_name is None:
            with self._db_handle_lock:
                db_name = next(
                    (
                        name
                        for name, handle in self.db_handle.items()
                        if str(handle) == str(db_handle)
                    ),
                    None,
                )

        if db_name and not params.get("db_handle"):
            params["db_handle"] = self.GetDBHandle(db_name)

        response = self.post(WECHAT_DATABASE_QUERY , QueryDatabaseBody(**params))
        if not isinstance(response, dict) or response.get("err_msg") not in self.DB_HANDLE_ERRORS:
            return response

        self.invalidate_db_handles()
        if not db_name:
            return response

        fresh_handle = self.GetDBHandle(db_name)
        if not fresh_handle:
            return response

        params["db_handle"] = fresh_handle
        retry = self.post(WECHAT_DATABASE_QUERY , QueryDatabaseBody(**params))
        if isinstance(retry, dict) and retry.get("err_msg") in self.DB_HANDLE_ERRORS:
            self.invalidate_db_handles()
        return retry

    def InvalidateDatabaseHandles(self , **params) -> Dict:
        return self.post(
            WECHAT_DATABASE_INVALIDATE_HANDLES,
            InvalidateDatabaseHandlesBody(**params),
        )

    def SetVersion(self , **params) -> Dict:
        return self.post(WECHAT_SET_VERSION , SetVersionBody(**params))

    def StartLogHook(self , **params) -> Dict:
        return self.post(WECHAT_LOG_START_HOOK , StartLogHookBody(**params))

    def StopLogHook(self , **params) -> Dict:
        return self.post(WECHAT_LOG_STOP_HOOK , StopLogHookBody(**params))

    def OpenBrowserWithUrl(self , **params) -> Dict:
        return self.post(WECHAT_BROWSER_OPEN_WITH_URL , OpenBrowserWithUrlBody(**params))

    def GetPublicMsg(self , **params) -> Dict:
        return self.post(WECHAT_GET_PUBLIC_MSG , GetPublicMsgBody(**params))

    def ForwardMessage(self , **params) -> Dict:
        return self.post(
            WECHAT_MSG_FORWARD_MESSAGE,
            ForwardMessageBody(**params),
            timeout=self.send_timeout,
        )

    def GetQrcodeImage(self , **params):
        r = requests.post(
            self._api_url(WECHAT_GET_QRCODE_IMAGE),
            data=GetQrcodeImageBody(**params).json(),
            timeout=self.request_timeout,
        )
        return r.content

    def GetA8Key(self , **params) -> Dict:
        return self.post(WECHAT_GET_A8KEY , GetA8KeyBody(**params))

    def SendXml(self , **params) -> Dict:
        return self.post(WECHAT_MSG_SEND_XML , SendXmlBody(**params), timeout=self.send_timeout)

    def LogOut(self , **params) -> Dict:
        return self.post(WECHAT_LOGOUT , LogOutBody(**params))

    def GetTransfer(self , **params) -> Dict:
        return self.post(WECHAT_GET_TRANSFER , GetTransferBody(**params))

    def SendEmotion(self , **params) -> Dict:
        return self.post(
            WECHAT_MSG_SEND_EMOTION,
            SendEmotionBody(**params),
            timeout=self.send_timeout,
        )

    def GetCdn(self , **params) -> Dict:
        return self.post(WECHAT_GET_CDN , GetCdnBody(**params))

    def MarkAsRead(self , **params) -> Dict:
        return self.post(WECHAT_MSG_MARK_AS_READ , MarkAsReadBody(**params))

    #[自定义
    def invalidate_db_handles(self) -> None:
        with self._db_handle_lock:
            self.db_handle.clear()
        try:
            self.InvalidateDatabaseHandles()
        except Exception:
            pass

    def GetDBHandle(self, db_name="MicroMsg.db") -> int:
        with self._db_handle_lock:
            cached = self.db_handle.get(db_name)
        if cached:
            return cached

        handles = self.GetDatabaseHandles().get("data", [])
        refreshed = {
            item["db_name"]: item["handle"]
            for item in handles
            if isinstance(item, dict) and item.get("db_name") and item.get("handle")
        }
        with self._db_handle_lock:
            self.db_handle.update(refreshed)
            return self.db_handle.get(db_name, 0)

    def GetContactListBySql(self) -> Dict:
        sql = "select UserName,Alias,Remark,NickName,Type from Contact"   #  where type!=4 and type!=0;
        ContactList = self.QueryDatabase(db_name="MicroMsg.db", sql=sql)["data"]
        contact_data = {}         # {wxid : {alias, remark, nickname , type}}
        for index in range(1, len(ContactList)):
            wxid = ContactList[index][0]
            contact_data[wxid] = {}
            contact_data[wxid]['alias'] = ContactList[index][1]
            contact_data[wxid]['remark'] = ContactList[index][2]
            contact_data[wxid]['nickname'] = ContactList[index][3]
            contact_data[wxid]['type'] = ContactList[index][4]

        sql = "select UserName,'' as Alias,Remark,NickName,Type from OpenIMContact"   #  where type!=4 and type!=0;
        OpenIMContactList = self.QueryDatabase(
            db_name="OpenIMContact.db", sql=sql
        )["data"]
        for index in range(1, len(OpenIMContactList)):
            wxid = OpenIMContactList[index][0]
            contact_data[wxid] = {}
            contact_data[wxid]['alias'] = OpenIMContactList[index][1]
            contact_data[wxid]['remark'] = OpenIMContactList[index][2]
            contact_data[wxid]['nickname'] = OpenIMContactList[index][3]
            contact_data[wxid]['type'] = OpenIMContactList[index][4]
        return contact_data

    def GetGroupMembersBySql(self, room_id) -> Dict:
        group_data = {} #{ "wxID" : "displayName"}
        sql = "select RoomData from ChatRoom where ChatRoomName = '" + room_id + "' ;"
        data = self.QueryDatabase(
            db_name="MicroMsg.db", sql=sql
        )["data"]
        chatroom = ChatRoom.ChatRoomData()
        group_member = {}
        chatroom.ParseFromString(bytes(base64.b64decode(data[1][0])))
        for k in chatroom.members:
            if k.displayName != "":
                group_member[k.wxID] = k.displayName
        return group_member

    def GetAllGroupMembersBySql(self) -> Dict:
        group_data = {} #{"group_id" : { "wxID" : "displayName"}}
        sql = "select ChatRoomName,RoomData from ChatRoom"
        GroupMemberList = self.QueryDatabase(
            db_name="MicroMsg.db", sql=sql
        )["data"]
        chatroom = ChatRoom.ChatRoomData()
        for index in range(1 , len(GroupMemberList)):
            group_member = {}
            chatroom.ParseFromString(bytes(base64.b64decode(GroupMemberList[index][1])))
            for k in chatroom.members:
                if k.displayName != "":
                    group_member[k.wxID] = k.displayName
            group_data[GroupMemberList[index][0]] = group_member
        return group_data

    def GetPictureBySql(self, wxid) -> Dict:
        if not is_openim_contact_id(wxid):
            sql = f"select usrName,smallHeadImgUrl,bigHeadImgUrl from ContactHeadImgUrl where usrName='{wxid}';" 
            result = self.QueryDatabase(db_name="MicroMsg.db", sql=sql)
        else:
            sql = f"select UserName,SmallHeadImgUrl,BigHeadImgUrl from OpenIMContact where UserName='{wxid}';" 
            result = self.QueryDatabase(db_name="OpenIMContact.db", sql=sql)
        try:
            if result["data"][1][2] != "":
                return result["data"][1][2]
            if result["data"][1][1] != "":
                return result["data"][1][1]
            return None
        except:
            return None

    def GetContactBySql(self, wxid):
        if not is_openim_contact_id(wxid):
            sql = f"select UserName,Alias,Remark,NickName,Type from Contact where UserName='{wxid}';" 
            result = self.QueryDatabase(db_name="MicroMsg.db", sql=sql)
        else:
            sql = f"select UserName,'' as Alias,Remark,NickName,Type from OpenIMContact where UserName='{wxid}';" 
            result = self.QueryDatabase(db_name="OpenIMContact.db", sql=sql)
        if len(result.get("data") or []) > 1:
            return result["data"][1]
        else:
            return None
    #自定义]

    def post(self , type : int, params : Body, timeout: Optional[float] = None) -> Dict:
        response = requests.post(
            self._api_url(type),
            data=params.json(),
            timeout=self.request_timeout if timeout is None else timeout,
        )
        return json.loads(response.content.decode("utf-8"), strict=False)

    def exec_command(self , item: str) -> Callable:
        return eval(f"self.{item}")

        
