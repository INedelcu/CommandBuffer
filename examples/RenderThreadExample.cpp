// A game loop with a render thread.
//
// The game thread simulates and records rendering commands; the render thread executes
// them. RenderCommandBuffer defines the commands: each producer function writes a command
// type and its payload, and ExecuteUntilStopped() reads them back on the render thread
// and switches on the type. The Renderer belongs to the render thread: the game thread
// never touches it. At most kMaxFramesInFlight frames are recorded but not yet rendered
// at any time, the usual way to bound latency and memory.

#include "cb/CommandBuffer.h"

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <thread>
#include <vector>

struct Color
{
    float r, g, b;
};

struct Transform
{
    float x, y, angle;
};

// Stand-in for a real graphics API wrapper. Lives on the render thread.
class Renderer
{
public:
    void SetClearColor(const Color& color) { m_ClearColor = color; }

    void CreateMesh(int mesh, const float* vertices, std::uint32_t vertexCount)
    {
        if (mesh >= static_cast<int>(m_Meshes.size()))
            m_Meshes.resize(mesh + 1);
        m_Meshes[mesh].assign(vertices, vertices + vertexCount);
    }

    void Draw(int mesh, const Transform& transform)
    {
        // A real renderer would bind buffers and issue a draw call here.
        for (float v : m_Meshes[mesh])
            m_Checksum += v * std::cos(transform.angle) + transform.x + transform.y;
        ++m_DrawCalls;
    }

    void Present(int frame)
    {
        if (frame % 60 == 0)
            std::printf("render thread: frame %3d, %d draws, clear color (%.2f, %.2f, %.2f)\n",
                        frame, m_DrawCalls, m_ClearColor.r, m_ClearColor.g, m_ClearColor.b);
        m_DrawCalls = 0;
    }

    double Checksum() const { return m_Checksum; }

private:
    std::vector<std::vector<float>> m_Meshes;
    Color m_ClearColor = {};
    int m_DrawCalls = 0;
    double m_Checksum = 0;
};

// The command types of this command buffer.
enum RenderCommand : cb::CommandType
{
    kCmd_SetClearColor = cb::kCmd_FirstUser, // payload: Color
    kCmd_CreateMesh,                         // payload: CreateMeshPayload, then vertexCount floats
    kCmd_Draw,                               // payload: DrawPayload
    kCmd_Present,                            // payload: int frame
};

// One plain struct per command with more than one field, written and read as a whole.
struct CreateMeshPayload
{
    int mesh;
    std::uint32_t vertexCount;
};

struct DrawPayload
{
    int mesh;
    Transform transform;
};

class RenderCommandBuffer : public cb::CommandBuffer
{
public:
    using CommandBuffer::CommandBuffer;

    // ------------------------------------------------------------- Game thread

    void SetClearColor(const Color& color) { WriteCommand(kCmd_SetClearColor, color); }

    // The vertex data is copied into the command buffer itself.
    void CreateMesh(int mesh, const float* vertices, std::uint32_t vertexCount)
    {
        WriteCommand(kCmd_CreateMesh, CreateMeshPayload{mesh, vertexCount});
        WriteArray(vertices, vertexCount);
    }

    void Draw(int mesh, const Transform& transform) { WriteCommand(kCmd_Draw, DrawPayload{mesh, transform}); }

    void Present(int frame) { WriteCommand(kCmd_Present, frame); }

    // ------------------------------------------------------------- Render thread

    void ExecuteUntilStopped(Renderer& renderer)
    {
        cb::CommandType type;
        while (ReadCommand(type))
        {
            switch (type)
            {
                case kCmd_SetClearColor:
                {
                    renderer.SetClearColor(ReadPayload<Color>());
                    break;
                }

                case kCmd_CreateMesh:
                {
                    const CreateMeshPayload& payload = ReadPayload<CreateMeshPayload>();
                    const float* vertices = ReadArray<float>(payload.vertexCount);
                    renderer.CreateMesh(payload.mesh, vertices, payload.vertexCount);
                    break;
                }

                case kCmd_Draw:
                {
                    const DrawPayload& payload = ReadPayload<DrawPayload>();
                    renderer.Draw(payload.mesh, payload.transform);
                    break;
                }

                case kCmd_Present:
                {
                    renderer.Present(ReadPayload<int>());
                    break;
                }

                default:
                {
                    assert(false && "unknown render command");
                    break;
                }
            }
            FinishCommand();
        }
    }
};

int main()
{
    constexpr int kFrames = 240;
    constexpr int kMaxFramesInFlight = 2;
    constexpr int kObjects = 100;

    RenderCommandBuffer commands(256 * 1024);
    Renderer renderer; // from here on, only touched by the render thread
    std::thread renderThread(&RenderCommandBuffer::ExecuteUntilStopped, &commands, std::ref(renderer));

    // Loading. The game thread picks the mesh handle; the render thread creates the mesh
    // when it gets to the command.
    const float triangle[] = {0.0f, 0.5f, -0.5f, -0.5f, 0.5f, -0.5f};
    const int triangleMesh = 0;
    commands.CreateMesh(triangleMesh, triangle, 6);

    std::vector<cb::CommandBuffer::Ticket> frameEnd(kFrames);
    for (int frame = 0; frame < kFrames; ++frame)
    {
        // Frame pacing: wait until the render thread has finished frame N - 2.
        if (frame >= kMaxFramesInFlight)
            commands.WaitUntilExecuted(frameEnd[frame - kMaxFramesInFlight]);

        // Game update, then record this frame's rendering.
        float time = frame / 60.0f;
        commands.SetClearColor(Color{0.5f + 0.5f * std::sin(time), 0.2f, 0.3f});
        for (int i = 0; i < kObjects; ++i)
            commands.Draw(triangleMesh, Transform{std::cos(time + i), std::sin(time + i), time * i});
        commands.Present(frame);
        frameEnd[frame] = commands.Submit();
    }

    commands.SubmitStop();
    renderThread.join(); // the Renderer is ours again
    std::printf("game thread: rendered %d frames, checksum %.3f\n", kFrames, renderer.Checksum());
    return 0;
}
