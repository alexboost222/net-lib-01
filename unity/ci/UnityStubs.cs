// Minimal stand-ins for the UnityEngine API the sample uses, so CI can compile the package
// sources with Unity's language version and profile (C# 9, .NET Standard 2.1) without Unity.
#pragma warning disable CS1591
namespace UnityEngine
{
    public class Object { public static void Destroy(Object o) { } }
    public class Component : Object { public Transform transform = new Transform(); public T GetComponent<T>() where T : new() => new T(); }
    public class Behaviour : Component { }
    public class MonoBehaviour : Behaviour { }
    public class Transform { public Vector3 position; }
    public class Material { public Color color; }
    public class Renderer { public Material material = new Material(); }
    public class GameObject : Object
    {
        public Transform transform = new Transform();
        public static GameObject CreatePrimitive(PrimitiveType t) => new GameObject();
        public T GetComponent<T>() where T : new() => new T();
    }
    public enum PrimitiveType { Cube }
    public struct Color { public static Color green => default; }
    public struct Vector3
    {
        public float x, y, z;
        public Vector3(float x, float y, float z) { this.x = x; this.y = y; this.z = z; }
        public static Vector3 operator *(Vector3 v, float f) => new Vector3(v.x * f, v.y * f, v.z * f);
        public static Vector3 operator +(Vector3 a, Vector3 b) => new Vector3(a.x + b.x, a.y + b.y, a.z + b.z);
        public static Vector3 Lerp(Vector3 a, Vector3 b, float t) => a;
    }
    public struct Rect { public Rect(float x, float y, float w, float h) { } }
    public static class Input { public static float GetAxis(string axis) => 0; }
    public static class Time { public static float deltaTime => 0; }
    public static class Application { public static bool isBatchMode => false; }
    public static class Debug { public static void Log(object m) { } public static void LogWarning(object m) { } }
    public static class GUI { public static void Label(Rect r, string s) { } }
    public sealed class TooltipAttribute : System.Attribute { public TooltipAttribute(string t) { } }
}
